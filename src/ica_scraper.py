import json
import logging
import pandas as pd
import urllib.request
from typing import Dict, List, Tuple
from itertools import product
from dataclasses import dataclass, asdict

import aiohttp
import asyncio

from src.base_sink import AbstractSink
from src.base_scraper import AbstractScraper


log = logging.getLogger(__name__)



@dataclass
class AccessToken:
    """Represents the bearer token response that needs to be supplied"""
    access_token: str
    expires_in: int


@dataclass
class IcaBankenResponse:
    """Response payload following successful API call"""
    list_interest_rate: float
    list_amount: int
    risk_discount_interest_rate: float
    risk_discount_amount: int
    loyalty_discount_interest_rate: float
    loyalty_discount_amount: int
    category_discount_interest_rate: float
    category_discount_amount: int
    offered_interest_rate: float
    offered_amount: int
    effective_interest_rate: float
    loan_to_value_interest_rate: int

    # already present upon request creation
    period: int
    loan_amount: float
    asset_value: float


@dataclass
class ParameterTriplet:
    """Ensure we can trace the url with the parameters"""
    period: int
    loan_amount: float
    asset_value: float


class IcaBankenScraper(AbstractScraper):
    """Scraper for https://www.icabanken.se"""

    provider = "ica"
    url_parameters: Dict[int, List[Tuple[int, int]]] = None
    access_token: AccessToken = None
    base_url = "https://www.icabanken.se/api"

    def __init__(self, sinks: List[AbstractSink], *args, **kwargs):
        self.parameter_matrix = self.generate_parameter_matrix()
        self.sinks = sinks
        self.refresh_access_token()

    def generate_parameter_matrix(self):
        """
        Generates a request parameter matrix for generating URLs
        """

        valid_periods_in_months = [3, 12, 36, 60]
        loan_amount_bins = [100_000 * i for i in range(1,101)] # min 100k max 10 mil.
        asset_value_bins = [100_000 * i for i in range(1,101)] # min 100k max 10 mil.
        combinations_of_bins = list(product(loan_amount_bins, asset_value_bins))
        parameter_matrix = {
            period: combinations_of_bins for period in valid_periods_in_months
        }
        return parameter_matrix

    def generate_scrape_urls(self) -> Tuple[List[str], List[ParameterTriplet]]:
        """Formats scraping urls based off of generated parameter matrix"""
        urls, parameters = [], []
        for period in self.parameter_matrix:
            for loan_amount, asset_amount in self.parameter_matrix[period]:
                triplet = ParameterTriplet(period, loan_amount, asset_amount)
                url = self.get_scrape_url(**asdict(triplet))
                urls.append(url)
                parameters.append(triplet)

        return urls, parameters
    
    def get_scrape_url(self, period: int, loan_amount: int, asset_value: int) -> str:
        return f"""https://apimgw-pub.ica.se/t/public.tenant/ica/bank/ac39/mortgage/1.0.0/interestproposal_v2_0?type_of_mortgage=BL&period_of_commitment={period}&loan_amount={loan_amount}&value_of_the_estate={asset_value}&ica_spend_amount=0"""

    def get_access_token(self) -> AccessToken:
        """Retrieves an access token to be used for auth against api"""
        url = self.base_url + "/token/public"
        with urllib.request.urlopen(url) as response:
            data = json.load(response)
        return AccessToken(**data)

    def refresh_access_token(self):
        """Util for refreshing access token and saving it"""
        self.access_token = self.get_access_token()

    def get_auth_header(self) -> str:
        """Util for defining valid auth header"""
        return {"Authorization": f"Bearer {self.access_token.access_token}"}

    def scrape_url(self, url: str) -> IcaBankenResponse:
        """Scrapes the json off of the provided url"""

        request = urllib.request.Request(url)
        request.add_header(*self.get_auth_header().split(": "))

        with urllib.request.urlopen(request) as response:
            data = json.load(response)["response"]
            return IcaBankenResponse(**data)
        
    async def fetch(self, session, url) -> IcaBankenResponse:
        async with session.get(url, headers=self.get_auth_header()) as response:
            return await response.json()
    
    async def fetch_urls(self, urls, event_loop):
        async with aiohttp.ClientSession(loop=event_loop) as session:
            results = await asyncio.gather(*[self.fetch(session, url) for url in urls], return_exceptions=True)
            return results

    def run_scraping_job(self, max_urls: int):
        """Manages the actual scraping job, exporting to each sink and so on"""
        urls, parameters = self.generate_scrape_urls()
        if max_urls < float("inf"):
            urls = urls[:max_urls]
        log.info(f"scraping {len(urls)} urls...")
        
        loop = asyncio.get_event_loop()
        responses = loop.run_until_complete(self.fetch_urls(urls, loop))
        serialized_data = []
        
        for i, (response, params) in enumerate(zip(responses, parameters)):
            serialized_data.append(IcaBankenResponse(**response["response"], **asdict(params)))
            if i % 100 == 0:
                log.info(f"completed {i} of {len(urls)} scrapes")

        log.info(f"successfully uncpacked {len(responses)}")
        export_df = pd.DataFrame.from_records(asdict(data) for data in serialized_data)

        log.info(f"Successfully scraped {len(export_df)}")
        log.info(f"exporting {self.sinks}")

        for s in self.sinks:
            log.info(f"exporting to {s}")
            s.export(export_df, name=self.provider)

    def __str__(self):
        return "IcaBankenScraper"

    def __repr__(self):
        return str(self)







    








