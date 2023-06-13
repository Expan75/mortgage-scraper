import logging
import numpy as np
import pandas as pd
from itertools import product
from typing import Dict, List, Optional, Tuple, Union
from dataclasses import dataclass, asdict

import aiohttp
import asyncio

from src.base_sink import AbstractSink
from src.base_scraper import AbstractScraper


log = logging.getLogger(__name__)


@dataclass
class QueryParameterPair:
    """Capture the parameters queried with"""
    loan_amount: float
    asset_value: float


@dataclass
class SBABResponse:
    """Response payload following successful API call"""
    LoptidText: str
    Rantesats: float
    Rantebindningstid: int
    EffektivRantesats: float
    
    # already present upon request creation
    loan_amount: float
    asset_value: float


class SBABScraper(AbstractScraper):
    """Scraper for https://sbab.se"""
    url_parameters: Optional[Dict[int, List[Tuple[int, int]]]] = None
    
    provider = 'sbab'
    base_url = "https://www.sbab.se/www-open-rest-api"
    
    def __init__(
        self, 
        sinks: List[AbstractSink], 
        proxy: str, 
        max_urls: Union[float,int]
    ):
        self.parameter_matrix = self.generate_parameter_matrix()
        self.sinks = sinks
        self.proxy = proxy
        self.max_urls = max_urls

    def generate_parameter_matrix(self):
        """Generates a request parameter matrix for generating URLs"""

        loan_amount_bins = [50_000 * i for i in range(1,201)] # min 50k max 10 mil.

        # construct asset_value_bins based off of .005 increments
        asset_value_bins = []
        for ltv in np.arange(0.005, 1, 0.005):
            for mortgage_amount in loan_amount_bins:
                asset_value = int(1 / (ltv / mortgage_amount))
                asset_value_bins.append(asset_value)

        parameter_matrix = list(product(loan_amount_bins, asset_value_bins))

        return parameter_matrix

    def generate_scrape_urls(self) -> Tuple[List[str], List[QueryParameterPair]]:
        """Formats scraping urls based off of generated parameter matrix"""
        urls, parameters = [], []
        for loan_amount, asset_amount in self.parameter_matrix:
            query_parmaeters = QueryParameterPair(loan_amount, asset_amount) 
            parameters.append(query_parmaeters)
            urls.append(self.get_scrape_url(loan_amount, asset_amount))

        return urls, parameters
    
    def get_scrape_url(self, loan_amount: int, estate_value: int) -> str:
        """Formats a scrape url based of 2-dim pricing parameters"""
        return (
            self.base_url
            + "/resources/rantor"
            + f"/bolan/hamtaprisdiffaderantor/{estate_value}/{loan_amount}"
        )
    
    async def fetch(self, session, url) -> dict:
        """Actual request sender; processes concurrently"""
        log.info("visiting: ", url)
        if self.proxy:
            async with session.get(url, proxy=self.proxy) as response:
                return await response.json()
        else:
            async with session.get(url) as response:
                return await response.json()
    
    async def fetch_urls(self, urls, event_loop):
        """Batches conccurent requests spread over the default conneciton poolsize"""
        async with aiohttp.ClientSession(loop=event_loop) as session:
            # gather needs to occur with spread operator or manually provide each arg!
            results = await asyncio.gather(
                *[self.fetch(session=session, url=url) for url in urls]
            )
            return results

    def run_scraping_job(self):
        """Manages the actual scraping job, exporting to each sink and so on"""
        
        urls, parameters = self.generate_scrape_urls()
        urls = urls[:self.max_urls]

        log.info(f"scraping {len(urls)} urls...")
        
        loop = asyncio.get_event_loop()
        responses = loop.run_until_complete(self.fetch_urls(urls, loop))
        serialized_data = []

        for i, (response, parameters) in enumerate(zip(responses, parameters)):
            for data in response:
                serialized_data.append(SBABResponse(**data, **asdict(parameters)))
            
            if i % 100 == 0:
                log.info(f"completed {i} of {len(urls)} scrapes")
    
        log.info(f"successfully uncpacked {len(urls)} requests")
        export_df = pd.DataFrame.from_records(asdict(data) for data in serialized_data)
        export_df['provider'] = self.provider

        log.info(f"Successfully scraped {len(export_df)}")
        log.info(f"exporting to {self.sinks}")

        for s in self.sinks:
            log.info(f"exporting to {s}")
            s.export(export_df, self.provider)

    def __str__(self):
        return "SBABScraper"

    def __repr__(self):
        return str(self)
