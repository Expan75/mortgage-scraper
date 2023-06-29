import logging
import datetime
from typing import Optional, List, Tuple
from dataclasses import dataclass, asdict

import requests
from tqdm import tqdm

from mortgage_scraper.scraper_config import ScraperConfig
from mortgage_scraper.base_sink import AbstractSink
from mortgage_scraper.base_scraper import AbstractScraper
from mortgage_scraper.segment import generate_segments

log = logging.getLogger(__name__)


@dataclass
class HypoteketResponse:
    """Response payload following successful API call"""

    interestTerm: str  # one of "threeMonth" | "oneYear" | "threeYear" | "fiveYear"
    rate: float
    effectiveInterestRate: float
    validFrom: datetime.datetime
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

    def generate_scrape_urls(self) -> List[str]:
        """Formats scraping urls based off of generated segments matrix"""
        urls = [
            self.get_scrape_url(int(segment.loan_amount), int(segment.asset_value))
            for segment in generate_segments()
        ]
        return urls

    def get_scrape_url(self, loan_amount: int, estate_value: int) -> str:
        return (
            f"{self.base_url}"
            + "/loans/interestRates"
            + f"?propertyValue={estate_value}&loanSize={loan_amount}"
        )

    def run_scraping_job(self):
        """Manages the actual scraping job, exporting to each sink and so on"""
        urls = self.generate_scrape_urls()
        if self.config.max_urls is not None:
            urls = urls[: self.config.max_urls]
        log.info(f"scraping {len(urls)} urls...")

        # given aggresive rate-limiting, defer to synchronous requests
        request_options = {"headers": {"Content-Type": "application/json"}}
        if self.config.proxy:
            protocol = "http" if "https" not in self.config.proxy else "https"
            request_options["proxies"] = {protocol: self.config.proxy}

        for url in tqdm(urls):
            response = requests.get(url, **request_options)

            if response.status_code != 200:
                log.critical(f"Hypoteket requests yield {response.status_code}")
            try:
                parsed = response.json()
                records = [
                    {**asdict(HypoteketResponse(**period)), "url": url}
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
