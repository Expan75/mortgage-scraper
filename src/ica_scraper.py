import os
import json
import logging
import numpy as np
import pandas as pd
import urllib.request
from typing import Dict, List, Tuple
from itertools import combinations, product
from dataclasses import dataclass

from src.base_sink import AbstractSink
from src.base_scraper import AbstractScraper


log = logging.getLogger(__file__)



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



class IcaBankenScraper(AbstractScraper):
    """Scraper for https://www.icabanken.se"""

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

        valid_periods_in_months = {3, 12, 12*3, 12*5}
        loan_amount_bins = [100_000 * i for i in range(1,101)] # min 100k max 10 mil.
        asset_value_bins = [100_000 * i for i in range(1,101)] # min 100k max 10 mil.
        combinations_of_bins = product(loan_amount_bins, asset_value_bins)

        return {
            period: combinations_of_bins for period in valid_periods_in_months
        }

    def generate_scrape_urls(self) -> List[str]:
        """Formats scraping urls based off of generated parameter matrix"""
        urls = []
        for period in self.parameter_matrix:
            for loan_amount, asset_amount in self.parameter_matrix[period]:
                url = self.get_scrape_url(period, loan_amount, asset_amount)
                urls.append(url)

        return urls
    
    def get_scrape_url(self, commitment_period: int, loan_amount: int, estate_value: int) -> str:
        return f"""https://apimgw-pub.ica.se/t/public.tenant/ica/bank/ac39/mortgage/1.0.0/interestproposal_v2_0?type_of_mortgage=BL&period_of_commitment={commitment_period}&loan_amount={loan_amount}&value_of_the_estate={estate_value}&ica_spend_amount=0"""

    def get_access_token(self) -> AccessToken:
        """Retrieves an access token to be used for auth against api"""
        url = os.path.join(self.base_url, "token/public")
        with urllib.request.urlopen(url) as response:
            data = json.load(response)
        return AccessToken(**data)

    def refresh_access_token(self):
        """Util for refreshing access token and saving it"""
        self.access_token = self.get_access_token()

    def get_auth_header(self) -> str:
        """Util for defining valid auth header"""
        return f"Authorization: Bearer {self.access_token.access_token}"

    def scrape_url(self, url: str) -> IcaBankenResponse:
        """Scrapes the json off of the provided url"""

        request = urllib.request.Request(url)
        request.add_header(*self.get_auth_header().split(": "))

        with urllib.request.urlopen(request) as response:
            data = json.load(response)["response"]
            return IcaBankenResponse(**data)

    def run_scraping_job(self):
        """Manages the actual scraping job, exporting to each sink and so on"""
        log.info("Starting IcaScraper...")
        
        urls = self.generate_scrape_urls()
        responses = [self.scrape_url(url) for url in urls]

        print(responses[0])
        export_df = pd.DataFrame.from_records(response.asdict() for response in responses)

        log.info(f"Successfully scraped {len(export_df)}")
        log.info("exporting to", self.sinks)

        for s in self.sinks:
            log.info("exporting to %s", s)
            s.export(export_df)

    def __str__(self):
        return "IcaBankenScraper"

    def __repr__(self):
        return str(self)







    








