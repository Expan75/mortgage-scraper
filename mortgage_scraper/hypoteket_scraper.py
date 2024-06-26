import time
import logging
import random
from datetime import datetime
from typing import Optional, List, Tuple, Union, Dict
from dataclasses import dataclass, asdict

import requests
from tqdm import tqdm

from mortgage_scraper.scraper_config import ScraperConfig
from mortgage_scraper.base_sink import AbstractSink
from mortgage_scraper.base_scraper import AbstractScraper
from mortgage_scraper.segment import generate_segments, MortgageMarketSegment

log = logging.getLogger(__name__)


@dataclass
class HypoteketResponse:
    """Response payload following successful API call"""

    interestTerm: str  # one of "threeMonth" | "oneYear" | "threeYear" | "fiveYear"
    rate: float
    effectiveInterestRate: float
    validFrom: datetime
    id: int
    order: int
    codeInterestRate: float
    codeEffectiveInterestRate: float
    code: str

    def get_interest_term_period_months(self) -> int:
        lookup: Dict[str, int] = {
            "threeMonth": 3,
            "sixMonth": 6,
            "oneYear": 12,
            "threeYear": 12 * 3,
            "fiveYear": 12 * 5,
            "tenYear": 12 * 10,
        }
        return lookup[self.interestTerm]


class HypoteketScraper(AbstractScraper):
    """Scraper for https://api.hypoteket.com"""

    provider = "hypoteket"
    url_parameters: Optional[List[Tuple[int, int]]] = None
    base_url = "https://api.hypoteket.com/api/v1"

    def __init__(
        self,
        sinks: List[AbstractSink],
        config: ScraperConfig,
    ):
        self.sinks = sinks
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({"Content-type": "application/json"})

        if config.proxies:
            self.session.proxies.update(self.config.proxies_by_protocol)

    def generate_scrape_urls(self) -> Tuple[List[str], List[MortgageMarketSegment]]:
        """Formats scraping urls based off of generated segments matrix"""

        segments = generate_segments(config=self.config)

        if self.config.randomize_url_order:
            seed = (
                self.config.seed
                if self.config.seed is not None
                else random.randint(1, 1000)
            )
            random.Random(seed).shuffle(segments)

        segments = segments[: self.config.urls_limit]
        urls = [self.get_scrape_url(s.loan_amount, s.asset_value) for s in segments]
        return urls, segments

    def get_scrape_url(
        self, loan_amount: Union[int, float], estate_value: Union[int, float]
    ) -> str:
        return (
            f"{self.base_url}"
            + "/loans/interestRates"
            + f"?propertyValue={int(estate_value)}&loanSize={int(loan_amount)}"
        )

    def run_scraping_job(self):
        """Manages the actual scraping job, exporting to each sink and so on"""
        urls, segments = self.generate_scrape_urls()
        log.info(f"scraping {len(urls)} urls...")

        url_segment_pairs = list(zip(urls, segments))
        for url, segment in tqdm(url_segment_pairs):
            time.sleep(self.config.delay)

            if self.config.rotate_user_agent:
                self.session.headers.update(self.config.get_random_user_agent_header())

            response = self.session.get(url)

            if response.status_code != 200:
                log.critical(f"Hypoteket requests yield {response.status_code}")
            try:
                parsed = response.json()
                records = []
                for period in parsed:
                    serialized = HypoteketResponse(**period)
                    record = {
                        "url": url,
                        **asdict(segment),
                        **asdict(serialized),
                        "period": serialized.get_interest_term_period_months(),
                        "offered_interest_rate": serialized.rate,
                    }
                    records.append(record)

                for sink in self.sinks:
                    for record in records:
                        sink.write(record)

            except requests.exceptions.JSONDecodeError:
                log.critical("could not parse request body as valid json, skipping")
            except NameError as e:
                print(e)
                log.critical(f"could not parse entries in json body: {response.json()}")

        # TODO: this is ugly
        for sink in self.sinks:
            sink.close()

    def __str__(self):
        return "HypoteketScraper"

    def __repr__(self):
        return str(self)
