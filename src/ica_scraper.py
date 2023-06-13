import logging
from typing import (
    Dict,
    List,
    Optional,
    Tuple,
    Union
)
from itertools import product
from dataclasses import dataclass, asdict

import aiohttp
import asyncio
import requests
import pandas as pd

from src.base_sink import AbstractSink
from src.base_scraper import AbstractScraper


log = logging.getLogger(__name__)


@dataclass
class AccessTokenResponse:
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
    url_parameters: Optional[Dict[int, List[Tuple[int, int]]]] = None
    access_token: str
    base_url = "https://www.icabanken.se/api"

    def __init__(self, sinks: List[AbstractSink], max_urls: int, proxy: str):
        self.proxy = proxy
        self.parameter_matrix = self.generate_parameter_matrix()
        self.sinks = sinks
        self.max_urls = max_urls
        self.refresh_access_token()

    @property
    def proxy_supports_https(self) -> bool:
        return "https" in self.proxy

    @property
    def proxy_supports_basic_auth(self) -> bool:
        return len(self.proxy.split(":")[0].split("@")) > 1
   
    @property
    def auth_header(self) -> dict[str,str]:
        return { "Authorization": f"Bearer {self.access_token}" }

    def generate_parameter_matrix(self):
        """Generates a request parameter matrix for generating URLs"""
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

    def get_access_token(self) -> str:
        """Retrieves an access token to be used for auth against api"""
        request = { 
            "url": self.base_url + "/token/public",
        }
        if self.proxy and self.proxy_supports_https:
            request["proxies"] = { "https": self.proxy }
        elif self.proxy:
            request["proxies"] = { "http": self.proxy }
        
        response = requests.get(**request)
        token_response = AccessTokenResponse(**response.json())
        return token_response.access_token

    def refresh_access_token(self):
        """Util for refreshing access token and saving it"""
        self.access_token = self.get_access_token()

    async def fetch(self, session, url) -> IcaBankenResponse:
        options: Dict[str, Union[dict,str]] = { "headers": self.auth_header }
        if self.proxy:
            options["proxy"] = self.proxy
        async with session.get(url, **options) as response:
            return await response.json()
    
    async def fetch_urls(self, urls, event_loop):
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
        print(responses[:2])
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







    








