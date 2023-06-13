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
from src.segment import MortgageMarketSegment, generate_segments


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
    

class SBABScraper(AbstractScraper):
    """Scraper for https://sbab.se"""
    url_parameters: Optional[Dict[int, List[Tuple[int, int]]]] = None
    
    provider = 'sbab'
    base_url = "https://www.sbab.se/www-open-rest-api"
    
    def __init__(
        self, 
        sinks: List[AbstractSink], 
        proxy: str, 
        max_urls: Optional[int] = None
    ):
        self.sinks = sinks
        self.proxy = proxy
        self.max_urls = max_urls

    
    def get_scrape_url(self, loan_amount: int, estate_value: int) -> str:
        """Formats a scrape url based of 2-dim pricing parameters"""
        return (
            self.base_url
            + "/resources/rantor"
            + f"/bolan/hamtaprisdiffaderantor/{int(estate_value)}/{int(loan_amount)}"
        )
    
    def generate_scrape_urls(self) -> Tuple[List[str], List[MortgageMarketSegment]]:
        """Formats scraping urls based off of generated parameter matrix"""
        segments = generate_segments()
        urls = [self.get_scrape_url(s.loan_amount, s.asset_value) for s in segments]
        return urls, segments
    
    async def fetch(self, session, url) -> dict:
        """Actual request sender; processes concurrently"""
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
        
        urls, segments = self.generate_scrape_urls()
        if self.max_urls is not None:
            urls = urls[:self.max_urls]

        log.info(f"scraping {len(urls)} urls...")
        
        loop = asyncio.get_event_loop()
        responses = loop.run_until_complete(self.fetch_urls(urls, loop))
        
        exportable_records = []
        for response, segment in zip(responses, segments):  
            serialized_data = [SBABResponse(**data) for data in response]
            for serialized in serialized_data:
                record = { **asdict(serialized), **asdict(segment) }
                exportable_records.append(record)
            
        export_df = pd.DataFrame.from_records(exportable_records)
        log.info(f"Successfully scraped {len(export_df)}")
        for s in self.sinks:
            log.info(f"exporting to {s}")
            s.export(export_df, self.provider)

    def __str__(self):
        return "SBABScraper"

    def __repr__(self):
        return str(self)
