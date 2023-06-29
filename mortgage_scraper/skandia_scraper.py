import time
import logging
import requests
from typing import Optional, Dict, List, Tuple, Any
from itertools import product
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
    def binding_period(self) -> str:
        return self.id.split(";")[0]

    @property
    def housing_interest(self) -> str:
        return self.id.split(";")[-1]


@dataclass
class RequestBody:
    # available at request formation
    bindingPeriod: int
    housingInterest: float
    loanVolume: float
    price: float


@dataclass
class SkandiaBankenResponse:
    """Response payload following successful API call"""

    AmortizePercentage: float
    AmortizeAmount: float
    Discount: float
    Interest: float
    BaseDicount: float
    EffectiveInterestRate: float
    YearlyDiscount: float
    MonthlyDiscount: float
    MonthlyInterestCost: float
    MonthlyInterestTaxDeduction: float
    AdditonalDiscounts: dict

    # available at request formation
    bindingPeriod: int
    housingInterest: float
    loanVolume: int
    price: int


class SkandiaBankenScraper(AbstractScraper):
    """Scraper for https://www.skandia.se/epi-api"""

    provider = "skandia"
    url_parameters: Optional[Dict[int, List[Tuple[int, int]]]] = None
    base_url = "https://www.skandia.se/epi-api"

    def __init__(
        self,
        sinks: List[AbstractSink],
        config: ScraperConfig,
    ):
        self.parameter_matrix = self.generate_parameter_matrix()
        self.sinks = sinks
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({"Content-type": "application/json"})

        if config.proxy:
            protocol = "https" if "https" in config.proxy else "http"
            self.session.proxies.update({protocol: config.proxy})

    def generate_parameter_matrix(self):
        """
        Generates a request parameter matrix for generating URLs
        """
        data = requests.get("https://www.skandia.se/epi-api/interests/mortgage").json()
        housing_interest: List[RateListEntry] = [
            RateListEntry(**entry) for entry in data
        ]
        loan_amount_bins = [100_000 * i for i in range(1, 101)]  # min 100k max 10 mil.
        asset_value_bins = [100_000 * i for i in range(1, 101)]  # min 100k max 10 mil.
        combinations_of_bins = product(loan_amount_bins, asset_value_bins)

        return {
            rate_list_entry.id: combinations_of_bins
            for rate_list_entry in housing_interest
        }

    def generate_scrape_body(
        self, period: str, housing_interest: str, loan_volume: int, price: int
    ) -> RequestBody:
        """As this API requires POSTs we opt for bodies instead of url parameters"""
        return RequestBody(period, housing_interest, loan_volume, price)

    def generate_scrape_bodies(self) -> List[RequestBody]:
        """As this API requires POSTs we opt for bodies instead of url parameters"""
        period_entries_response = self.session.get(
            "https://www.skandia.se/epi-api/interests/mortgage"
        ).json()
        parsed_entries: List[RateListEntry] = [
            RateListEntry(**res) for res in period_entries_response
        ]

        bodies: List[RequestBody] = []
        for entry in parsed_entries:
            period_segments: List[MortgageMarketSegment] = generate_segments(
                period=entry.binding_period
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
            bodies.extend(period_bodies)

        return bodies

    def run_scraping_job(self) -> None:
        """Manages the actual scraping job, exporting to each sink and so on"""
        bodies = self.generate_scrape_bodies()  # params here

        urls = ["https://www.skandia.se/papi/mortgage/v2.0/discounts" for _ in bodies]
        if self.config.max_urls is not None:
            urls = urls[: self.config.max_urls]

        log.info(f"scraping {len(urls)} urls...")
        urls_bodies_pairs = list(zip(urls, bodies))
        urls_bodies_pairs_retry: List[Any] = []

        for url, body in tqdm(urls_bodies_pairs):
            # skandia has aggresive rate limiting
            time.sleep(1)
            response = self.session.post(url, asdict(body))

            if response.status_code != 200:
                log.critical(f"request to Skandia yielded {response.status_code}")
                urls_bodies_pairs_retry.append((url, body))
            else:
                try:
                    parsed = response.json()
                    serialized = SkandiaBankenResponse(**parsed)
                    record = {**asdict(serialized), "url": url, **body}

                    for s in self.sinks:
                        s.write(record)
                except requests.exceptions.JSONDecodeError as e:
                    if "Vi har stoppat detta anrop" in response.text:
                        log.critical("request was blocked by Skandia")
                        log.critical("scraper is now likely ip blocked, exiting...")
                        break
                    else:
                        log.critical("could not decode json response, adding to retry")
                        print("error: ", e)
                        print("response", response.text)

        log.info(f"Scraped {len(urls)} out of {len(urls_bodies_pairs_retry)} urls")

        for s in self.sinks:
            s.close()

    def __str__(self):
        return "SkandiaBankenScraper"

    def __repr__(self):
        return str(self)