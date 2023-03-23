import json
import logging
import grequests
import pandas as pd
from itertools import product
from typing import Dict, List, Tuple
from dataclasses import dataclass, asdict

from src.base_sink import AbstractSink
from src.base_scraper import AbstractScraper


log = logging.getLogger(__name__)


@dataclass
class SBABResponse:
    """Response payload following successful API call"""
    loptidText: str
    Rantesats: float
    Rantebindningstid: int
    EffektivRantesats: float


class SBABScraper(AbstractScraper):
    """Scraper for https://sbab.se"""

    url_parameters: Dict[int, List[Tuple[int, int]]] = None
    base_url = "https://www.sbab.se/www-open-rest-api"

    def __init__(self, sinks: List[AbstractSink], *args, **kwargs):
        self.parameter_matrix = self.generate_parameter_matrix()
        self.sinks = sinks

    def generate_parameter_matrix(self):
        """
        Generates a request parameter matrix for generating URLs
        """

        loan_amount_bins = [50_000 * i for i in range(1,201)] # min 100k max 10 mil.
        asset_value_bins = [50_000 * i for i in range(1,201)] # min 100k max 10 mil.
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
        return self.base_url + f'/resources/rantor/bolan/hamtaprisdiffaderantor/{loan_amount}/{estate_value}'

    def run_scraping_job(self):
        """Manages the actual scraping job, exporting to each sink and so on"""
        
        urls = self.generate_scrape_urls()
        log.info(f"scraping {len(urls)} urls...")
        
        requests = (grequests.get(url) for url in urls)
        responses = grequests.map(requests)
        serialized_data = []

        for i, response in enumerate(responses):
            parsed_response = json.loads(response.text)
            for data in parsed_response:
                serialized_data.append(SBABResponse(**data))
            
            if i % 100 == 0:
                log.info(f"completed {i} of {len(urls)} scrapes")
    
        log.info(f"successfully uncpacked {len(urls)} requests")
        export_df = pd.DataFrame.from_records(asdict(data) for data in serialized_data)
        export_df.name = "sbab"

        log.info(f"Successfully scraped {len(export_df)}")
        log.info(f"exporting to {self.sinks}")

        for s in self.sinks:
            log.info(f"exporting to {s}")
            s.export(export_df)

    def __str__(self):
        return "SBABScraper"

    def __repr__(self):
        return str(self)