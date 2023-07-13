import time
import random
import logging
from typing import List, Tuple, Union
from dataclasses import dataclass, asdict

import requests
from tqdm import tqdm

from mortgage_scraper.base_sink import AbstractSink
from mortgage_scraper.base_scraper import AbstractScraper
from mortgage_scraper.segment import MortgageMarketSegment, generate_segments
from mortgage_scraper.scraper_config import ScraperConfig


log = logging.getLogger(__name__)


@dataclass
class SBABResponse:
    """Response payload following successful API call"""

    LoptidText: str
    Rantesats: float
    Rantebindningstid: int
    EffektivRantesats: float


class SBABScraper(AbstractScraper):
    """Scraper for https://sbab.se"""

    provider = "sbab"
    base_url = "https://www.sbab.se/www-open-rest-api"

    def __init__(self, sinks: List[AbstractSink], config: ScraperConfig):
        self.sinks = sinks
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({"Content-type": "application/json"})

    def get_scrape_url(
        self, loan_amount: Union[float, int], estate_value: Union[float, int]
    ) -> str:
        """Formats a scrape url based of 2-dim pricing parameters"""
        return (
            self.base_url
            + "/resources/rantor"
            + f"/bolan/hamtaprisdiffaderantor/{int(estate_value)}/{int(loan_amount)}"
        )

    def generate_scrape_urls(self) -> Tuple[List[str], List[MortgageMarketSegment]]:
        """Formats scraping urls based off of generated parameter matrix"""
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

    def run_scraping_job(self):
        """Manages the actual scraping job, exporting to each sink and so on"""
        urls, segments = self.generate_scrape_urls()
        log.info(f"scraping {len(urls)} urls...")

        url_segment_pairs = list(zip(segments, urls))
        for segment, url in tqdm(url_segment_pairs):
            time.sleep(self.config.delay)

            if self.config.rotate_user_agent:
                self.session.headers.update(self.config.get_random_user_agent_header())

            response = self.session.get(url).json()
            serialized_data = [SBABResponse(**data) for data in response]
            for serialized in serialized_data:
                record = {
                    "url": url,
                    **asdict(serialized),
                    **asdict(segment),
                    "period": serialized.Rantebindningstid,
                    "bank": "sbab",
                }
                for sink in self.sinks:
                    sink.write(record)

        log.info("scrape job for sbab finished, closing sinks")
        for s in self.sinks:
            s.close()

    def __str__(self):
        return "SBABScraper"

    def __repr__(self):
        return str(self)
