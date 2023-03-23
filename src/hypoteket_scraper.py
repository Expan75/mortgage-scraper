import logging
import datetime
import pandas as pd
from itertools import product
from typing import Dict, List, Tuple
from dataclasses import dataclass, asdict

import requests

from src.base_sink import AbstractSink
from src.base_scraper import AbstractScraper


log = logging.getLogger(__name__)



@dataclass
class HypoteketResponse:
    """Response payload following successful API call"""
    interestTerm: str # one of "threeMonth" | "oneYear" | "threeYear" | "fiveYear"
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

    url_parameters: List[Tuple[int, int]] = None
    base_url = "https://api.hypoteket.com/api/v1"

    def __init__(self, sinks: List[AbstractSink], *args, **kwargs):
        self.parameter_matrix = self.generate_parameter_matrix()
        self.sinks = sinks

    def generate_parameter_matrix(self):
        """
        Generates a request parameter matrix for generating URLs
        """
        loan_amount_bins = [100_000 * i for i in range(1,101)] # min 100k max 10 mil.
        asset_value_bins = [100_000 * i for i in range(1,101)] # min 100k max 10 mil.
        parameter_matrix = product(loan_amount_bins, asset_value_bins)

        return parameter_matrix

    def generate_scrape_urls(self) -> List[str]:
        """Formats scraping urls based off of generated parameter matrix"""
        urls = []
        for loan_amount, asset_amount in self.parameter_matrix:
            url = self.get_scrape_url(loan_amount, asset_amount)
            urls.append(url)

        return urls
    
    def get_scrape_url(self, loan_amount: int, estate_value: int) -> str:
        return f"""{self.base_url + f'/loans/interestRates?propertyValue={estate_value}&loanSize={loan_amount}'}"""

    def run_scraping_job(self, max_urls: int):
        """Manages the actual scraping job, exporting to each sink and so on"""
        
        urls = self.generate_scrape_urls()
        if max_urls < float("inf"):
            urls = urls[:max_urls]
        log.info(f"scraping {len(urls)} urls...")

        # given aggresive rate-limiting, defer to synchronous requests
        responses = [requests.get(url).json() for url in urls]
        serialized_data = []
        
        for i, response in enumerate(responses):
            for data in response: 
                serialized_data.append(HypoteketResponse(**data))
            if i % 100 == 0:
                log.info(f"completed {i} of {len(urls)} scrapes")
    
        log.info(f"successfully uncpacked {len(urls)} requests")
        export_df = pd.DataFrame.from_records(asdict(data) for data in serialized_data)
        export_df.name = "hypoteket"

        log.info(f"Successfully scraped {len(export_df)}")
        log.info(f"exporting to {self.sinks}")

        for s in self.sinks:
            log.info(f"exporting to {s}")
            s.export(export_df)

    def __str__(self):
        return "HypoteketScraper"

    def __repr__(self):
        return str(self)