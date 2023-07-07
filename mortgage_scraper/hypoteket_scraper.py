import time
import logging
import random
from datetime import datetime
from typing import Optional, List, Tuple
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
        segments = generate_segments()

        if self.config.randomize_url_order:
            seed = (
                self.config.seed
                if self.config.seed is not None
                else random.randint(1, 1000)
            )
            random.Random(seed).shuffle(segments)

        urls = [
            self.get_scrape_url(int(s.loan_amount), int(s.asset_value))
            for s in segments[: self.config.urls_limit]
        ]
        return urls, segments

    def get_scrape_url(self, loan_amount: int, estate_value: int) -> str:
        return (
            f"{self.base_url}"
            + "/loans/interestRates"
            + f"?propertyValue={estate_value}&loanSize={loan_amount}"
        )

    def run_scraping_job(self):
        """Manages the actual scraping job, exporting to each sink and so on"""
        urls, segments = self.generate_scrape_urls()
        log.info(f"scraping {len(urls)} urls...")

        url_segment_pairs = zip(urls, segments)
        for url, segment in tqdm(url_segment_pairs):
            time.sleep(self.config.delay)
            response = self.session.get(url)

            if response.status_code != 200:
                log.critical(f"Hypoteket requests yield {response.status_code}")
            try:
                parsed = response.json()
                records = [
                    {
                        "url": url,
                        "scraped_at": datetime.now(),
                        **asdict(segment),
                        **asdict(HypoteketResponse(**period)),
                    }
                    for period in parsed
                ]

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
