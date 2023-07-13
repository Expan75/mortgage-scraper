import time
import random
import logging
import requests
from pprint import pprint
from typing import Optional, Dict, List, Tuple
from dataclasses import dataclass, asdict

from tqdm import tqdm
from mortgage_scraper.base_sink import AbstractSink
from mortgage_scraper.base_scraper import AbstractScraper
from mortgage_scraper.segment import MortgageMarketSegment, generate_segments
from mortgage_scraper.scraper_config import ScraperConfig

log = logging.getLogger(__name__)


@dataclass
class RateListEntry:
    """Represents high level mortgage rate listed on /mortgage"""

    id: str  # e.g. '3;4,41' # probably internal reference of some sort
    text: str  # "Ordinarie ränta (1 år): 5,19%"

    @property
    def binding_period(self) -> int:
        cleaned_value = self.id.split(";")[0]
        return int(cleaned_value)

    @property
    def housing_interest(self) -> float:
        cleaned_value = self.id.split(";")[-1].replace(",", ".")
        return float(cleaned_value)


@dataclass
class RequestBody:
    # available at request formation
    bindingsPeriod: int
    housingInterest: float
    loanVolume: int
    price: int

    # recently added
    hasOccupationalPension: Optional[bool] = False


@dataclass
class SkandiaBankenResponse:
    """Response payload following successful API call"""

    AmortizePercentage: float
    AmortizeAmount: float
    Discount: float
    Interest: float
    BaseDiscount: float
    EffectiveInterestRate: float
    YearlyDiscount: float
    MonthlyDiscount: float
    MonthlyInterestCost: float
    MonthlyInterestTaxDeduction: float
    AdditionalDiscounts: dict


class SkandiaBankenScraper(AbstractScraper):
    """Scraper for https://www.skandia.se/epi-api"""

    provider = "skandia"
    url_parameters: Optional[Dict[int, List[Tuple[int, int]]]] = None
    base_url = "https://www.skandia.se/epi-api"

    # expoential backoff
    retries: int = 0
    timeout: int = 0

    def __init__(
        self,
        sinks: List[AbstractSink],
        config: ScraperConfig,
    ):
        self.sinks = sinks
        self.config = config
        self.session = requests.Session()
        self.session.headers.update(
            {
                "accept": "application/json, text/plain, */*",
                "content-type": "application/json",
                "accept-language": "en-US,en;q=0.9,sv;q=0.8",
            }
        )

        if self.config.proxies:
            self.session.proxies.update(self.config.proxies_by_protocol)

    def generate_scrape_body(
        self, period: int, housing_interest: float, loan_volume: int, price: int
    ) -> RequestBody:
        """As this API requires POSTs we opt for bodies instead of url parameters"""
        return RequestBody(period, housing_interest, loan_volume, price)

    def generate_scrape_bodies(
        self,
    ) -> Tuple[Tuple[RequestBody], Tuple[MortgageMarketSegment]]:
        """As this API requires POSTs we opt for bodies instead of url parameters"""
        period_entries_response = self.session.get(
            "https://www.skandia.se/epi-api/interests/mortgage"
        ).json()
        parsed_entries: List[RateListEntry] = [
            RateListEntry(**res) for res in period_entries_response
        ]

        bodies: List[RequestBody] = []
        segments: List[MortgageMarketSegment] = []
        for entry in parsed_entries:
            period_segments: List[MortgageMarketSegment] = generate_segments(
                period=entry.binding_period, config=self.config
            )
            period_bodies = [
                self.generate_scrape_body(
                    entry.binding_period,
                    entry.housing_interest,
                    int(segment.loan_amount),
                    int(segment.asset_value),
                )
                for segment in period_segments
            ]
            segments.extend(period_segments)
            bodies.extend(period_bodies)

        body_segment_pairs: List[Tuple[RequestBody, MortgageMarketSegment]] = list(
            zip(bodies, segments)
        )

        if self.config.randomize_url_order:
            seed = (
                self.config.seed
                if self.config.seed is not None
                else random.randint(1, 1000)
            )
            random.Random(seed).shuffle(body_segment_pairs)

        # VERY ugly if proper ryping is to be applied to inverse zipssee
        # https://stackoverflow.com/questions/56564705/python-type-hints-for-generic-args-specifically-zip-or-zipwit # noqa: E5
        inverse_zipped_pairs = zip(
            *body_segment_pairs[: self.config.urls_limit]
        )  # noqa: E5  # type: ignore
        bodies, segments = inverse_zipped_pairs  # type: ignore
        return bodies, segments  # type: ignore

    def run_scraping_job(self) -> None:
        """Manages the actual scraping job, exporting to each sink and so on"""
        bodies, segments = self.generate_scrape_bodies()  # params here
        urls = ["https://www.skandia.se/papi/mortgage/v2.0/discounts" for _ in bodies]

        log.info(f"scraping {len(urls)} urls...")
        url_body_segment_triples = list(zip(urls, bodies, segments))

        for url, body, segment in tqdm(url_body_segment_triples):
            time.sleep(self.config.delay)

            # user agent key is lowercase and header  always present in skandia's case
            (header, value), *_ = self.config.get_random_user_agent_header().items()
            adjusted_header = {header.lower(): value}
            self.session.headers.update(adjusted_header)

            # using session to spawn requests lead to added skandia rejected headers
            request = requests.Request(
                "POST", url=url, json=asdict(body), headers=self.session.headers
            ).prepare()

            # always attached by requests, but not accepted by skandia
            del request.headers["Accept-Encoding"]

            response = self.session.send(request)
            try:
                parsed = response.json()
                serialized = SkandiaBankenResponse(**parsed)
                record = {
                    "url": url,
                    **asdict(segment),
                    **asdict(serialized),
                    **asdict(body),
                    "bank": "skandia",
                }

                for s in self.sinks:
                    s.write(record)

                # reset backoff on successful request
                self.retries = 0
                self.timeout = 0

            except requests.exceptions.JSONDecodeError as e:
                blocked = "Vi har stoppat detta anrop" in response.text
                if blocked and self.retries > 5:
                    log.critical("request was blocked by Skandia, dumping request.")
                    log.critical(
                        f"exhausted exp. backoff strategy after {self.retries}"
                    )
                    log.critical("dumping request and exiting...")
                    log_error_dump = {
                        "url": url,
                        "body": asdict(body),
                        "method": "POST",
                        "headers": self.session.headers,
                    }
                    pprint({"request": log_error_dump})
                    exit(1)
                elif blocked:
                    log.critical(
                        f"request was blocked, recovering via exponential backoff with used retries {self.retries}/5 possible"  # noqa
                    )
                    self.retries = self.retries + 1
                    self.timeout = 2 * (2 ** (self.retries))
                    time.sleep(self.timeout)
                    url_body_segment_triples.append((url, body, segment))
                else:
                    raise e

        for s in self.sinks:
            s.close()

    def __str__(self):
        return "SkandiaBankenScraper"

    def __repr__(self):
        return str(self)
