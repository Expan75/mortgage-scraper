import csv
import time
import logging
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Tuple,
    Union
)
from datetime import datetime, timedelta
from itertools import product
from dataclasses import dataclass, asdict, fields

import aiohttp
import asyncio
import requests
import pandas as pd
from tqdm import tqdm

from src.base_sink import AbstractSink
from src.base_scraper import AbstractScraper
from src.segment import MortgageMarketSegment, generate_segments

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


class IcaBankenScraper(AbstractScraper):
    """Scraper for https://www.icabanken.se"""

    provider = "ica"
    url_parameters: Optional[Dict[int, List[Tuple[int, int]]]] = None
    base_url = "https://www.icabanken.se/api"
    max_urls: Optional[int]
   
    access_token: str
    token_last_updated_at: datetime

    def __init__(self, sinks: List[AbstractSink], proxy: str, max_urls: int):
        self.proxy = proxy
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
    def access_token_expired(self) -> bool: 
        token_expiry_date = self.token_last_updated_at + timedelta(minutes=1) 
        return datetime.now() > token_expiry_date

    def get_auth_header(self) -> dict[str, str]:
        if self.access_token_expired:
            self.refresh_access_token()
        return { "Authorization": f"Bearer {self.access_token}"}

    def get_access_token(self) -> str:
        """Retrieves an access token to be used for auth against api"""
        request: Dict[str, Union[Dict,str]] = { 
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
        self.token_last_updated_at = datetime.now()
        self.access_token = self.get_access_token()

    def get_scrape_url(
        self, 
        period: Any,
        loan_amount: Union[float,int],
        asset_value: Union[float,int]
    ) -> str:
        return (
            "https://apimgw-pub.ica.se/t/public.tenant/ica/bank/ac39/mortgage/1.0.0/interestproposal_v2_0?type_of_mortgage=BL"
            + f"&period_of_commitment={int(period)}"
            + f"&loan_amount={int(loan_amount)}"
            + f"&value_of_the_estate={int(asset_value)}"
            + "&ica_spend_amount=0"
        )

    def generate_scrape_urls(self) -> Tuple[List[str], List[MortgageMarketSegment]]:
        """Formats scraping urls based off of the default market segments"""
        segments: List[MortgageMarketSegment] = []
        periods = [str(p) for p in [3, 12, 36, 60]]
        for period in periods:            
            segments.extend(generate_segments(period))
        urls = [
            self.get_scrape_url(s.period, s.loan_amount, s.asset_value) 
            for s in segments
        ]
        return urls, segments

    def run_scraping_job(self):
        """Manages the actual scraping job, exporting to each sink and so on"""
        urls, segments = self.generate_scrape_urls()
        if self.max_urls is not None:
            urls = urls[:self.max_urls]
        log.info(f"scraping {len(urls)} urls...")
        
        with open("./ica.csv", "a+") as f:
            cols = [f.name for f in fields(IcaBankenResponse)]
            csv_writer = csv.DictWriter(f, [*cols, "url"])
            csv_writer.writeheader()
            for url in tqdm(urls):
                time.sleep(0.5)
                response = requests.get(url, headers=self.get_auth_header())
            
                if response.status_code == 405:
                    log.critical(
                        "status 405 despite refreshing token, retrying /w new token"
                    )
                    self.refresh_access_token()
                    response = requests.get(url, headers=self.get_auth_header())
          
                parsed_json = None
                try:
                    parsed_json = response.json()
                except requests.exceptions.JSONDecodeError:
                    log.info(
                        f"could not unpack response for {url=}, skipping {response=}"
                    )
    
                # seems certain bins are not answred at all, falling out of bounds
                if ( 
                    parsed_json.get("resmeta") is not None
                    and "FELAKTIGA" in parsed_json["resmeta"].get("be_meddel", "")
                ):
                    log.info("yielded 'felaktiga parameterar' error code")
                    log.info("skipping url", url)
                else:
                    serialized = IcaBankenResponse(**parsed_json["response"]) 
                    csv_writer.writerow({ **asdict(serialized), "url": url })
                f.flush()
        
    def __str__(self):
        return "IcaBankenScraper"

    def __repr__(self):
        return str(self)







    








