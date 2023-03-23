import json
import logging
import pandas as pd
import urllib.request
from typing import Dict, List, Tuple
from itertools import product
from dataclasses import dataclass, asdict

import requests
import aiohttp
import asyncio

from src.base_sink import AbstractSink
from src.base_scraper import AbstractScraper


log = logging.getLogger(__name__)



@dataclass
class RateListEntry:
    """Represents high level mortgage rate listed on /mortgage"""
    id: str # e.g. '3;4,41' # probably internal reference of some sort
    text: str # "Ordinarie ränta (1 år): 5,19%"
    

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


class SkandiaBankenScraper(AbstractScraper):
    """Scraper for https://www.skandia.se/epi-api"""

    url_parameters: Dict[int, List[Tuple[int, int]]] = None
    base_url = "https://www.skandia.se/epi-api"

    def __init__(self, sinks: List[AbstractSink], *args, **kwargs):
        self.parameter_matrix = self.generate_parameter_matrix()
        self.sinks = sinks

    def generate_parameter_matrix(self):
        """
        Generates a request parameter matrix for generating URLs
        """
        data = requests.get("https://www.skandia.se/epi-api/interests/mortgage").json()
        housing_interest: List[RateListEntry] = [RateListEntry(**entry) for entry in data]

        loan_amount_bins = [100_000 * i for i in range(1,101)] # min 100k max 10 mil.
        asset_value_bins = [100_000 * i for i in range(1,101)] # min 100k max 10 mil.
        combinations_of_bins = product(loan_amount_bins, asset_value_bins)

        return {
            rate_list_entry.id: combinations_of_bins for rate_list_entry in housing_interest
        }

    def generate_scrape_body(self) -> dict:
        """As this API requires POSTs we opt for bodies instead of url parameters""" 
        return {}

    def generate_scrape_bodies(self) -> List[dict]:
        """As this API requires POSTs we opt for bodies instead of url parameters"""
        bodies = []

        for key in self.parameter_matrix:
            bindingPeriod, housingInterest = key.strip().split(";")
            for loan_amount, asset_amount in self.parameter_matrix[key]:
                body = self.generate_scrape_body(bindingPeriod, housingInterest, loan_amount, asset_amount)
                bodies.append(body)

        return bodies
        
    async def fetch(self, session, url, body) -> SkandiaBankenResponse:
        async with session.post(url, data=body) as response:
            return await response.json()
    
    async def fetch_urls(self, urls, bodies, event_loop):
        async with aiohttp.ClientSession(loop=event_loop) as session:
            results = await asyncio.gather(*[self.fetch(session, url, body) for url, body in zip(urls, bodies)], return_exceptions=True)
            return results

    def run_scraping_job(self, max_urls: int):
        """Manages the actual scraping job, exporting to each sink and so on"""
        bodies = self.generate_scrape_bodies()
        urls = ["https://www.skandia.se/papi/mortgage/v2.0/discounts" for b in bodies]
        if max_urls < float("inf"):
            urls = urls[:max_urls]
        log.info(f"scraping {len(urls)} urls...")
        
        loop = asyncio.get_event_loop()
        responses = loop.run_until_complete(self.fetch_urls(urls, bodies, loop))
        serialized_data = []
        
        for i, response in enumerate(responses):
            serialized_data.append(SkandiaBankenResponse(**response["response"]))
            if i % 100 == 0:
                log.info(f"completed {i} of {len(urls)} scrapes")

        log.info(f"successfully uncpacked {len(responses)}")
        export_df = pd.DataFrame.from_records(asdict(data) for data in serialized_data)
        export_df.name = "SkandiaBankenScraper"

        log.info(f"Successfully scraped {len(export_df)}")
        log.info(f"exporting {self.sinks}")

        for s in self.sinks:
            log.info(f"exporting to {s}")
            s.export(export_df)

    def __str__(self):
        return "SkandiaBankenScraper"

    def __repr__(self):
        return str(self)







    








