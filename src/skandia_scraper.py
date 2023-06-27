import time
import logging
import pandas as pd
import requests
from typing import Optional, Dict, List, Tuple, Union
from itertools import product
from dataclasses import dataclass, asdict

from tqdm import tqdm
from src.base_sink import AbstractSink
from src.base_scraper import AbstractScraper
from src.segment import MortgageMarketSegment, generate_segments

log = logging.getLogger(__name__)



@dataclass
class RateListEntry:
    """Represents high level mortgage rate listed on /mortgage"""
    id: str # e.g. '3;4,41' # probably internal reference of some sort
    text: str # "Ordinarie ränta (1 år): 5,19%"

    @property
    def binding_period(self) -> str:
        return self.id.split(";")[0]

    @property
    def housing_interest(self) -> str:
        return self.id.split(";")[-1]


@dataclass
class RequestBody:
    # available at request formation
    bindingPeriod: int
    housingInterest: float
    loanVolume: float
    price: float


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

    # available at request formation
    bindingPeriod: int
    housingInterest: float
    loanVolume: int
    price: int


class SkandiaBankenScraper(AbstractScraper):
    """Scraper for https://www.skandia.se/epi-api"""

    provider = "skandia"
    url_parameters: Optional[Dict[int, List[Tuple[int, int]]]] = None
    base_url = "https://www.skandia.se/epi-api"

    def __init__(
        self,
        sinks: List[AbstractSink],
        proxy: str,
        max_urls: Optional[int] = None
    ):
        self.parameter_matrix = self.generate_parameter_matrix()
        self.sinks = sinks
        self.max_urls = max_urls
        self.proxy = proxy

    def generate_parameter_matrix(self):
        """
        Generates a request parameter matrix for generating URLs
        """
        data = requests.get("https://www.skandia.se/epi-api/interests/mortgage").json()
        housing_interest: List[RateListEntry] = [
            RateListEntry(**entry) for entry in data
        ]
        loan_amount_bins = [100_000 * i for i in range(1,101)] # min 100k max 10 mil.
        asset_value_bins = [100_000 * i for i in range(1,101)] # min 100k max 10 mil.
        combinations_of_bins = product(loan_amount_bins, asset_value_bins)

        return {
            rate_list_entry.id: combinations_of_bins 
            for rate_list_entry in housing_interest
        }

    def generate_scrape_body(
            self, 
            period: str, 
            housing_interest: str, 
            loan_volume: int,
            price: int
    ) -> RequestBody:
        """As this API requires POSTs we opt for bodies instead of url parameters""" 
        return RequestBody(period, housing_interest, loan_volume, price)

    def generate_scrape_bodies(self) -> List[RequestBody]:
        """As this API requires POSTs we opt for bodies instead of url parameters"""
        period_entries_response = requests.get(
            "https://www.skandia.se/epi-api/interests/mortgage"
        ).json()
        parsed_entries: List[RateListEntry] = [
            RateListEntry(**res) for res in period_entries_response
        ]
        
        bodies: List[RequestBody] = []
        for entry in parsed_entries:
            period_segments: List[MortgageMarketSegment] = generate_segments(
                period=entry.binding_period
            )
            period_bodies = [
                self.generate_scrape_body(
                    entry.binding_period,
                    entry.housing_interest,
                    int(segment.loan_amount),
                    int(segment.asset_value)
                ) for segment in period_segments]
            bodies.extend(period_segments)

        return bodies

    def run_scraping_job(self) -> None:
        """Manages the actual scraping job, exporting to each sink and so on"""
        bodies = self.generate_scrape_bodies() # params here
    
        urls = ["https://www.skandia.se/papi/mortgage/v2.0/discounts" for _ in bodies]
        if self.max_urls is not None:
            urls = urls[:self.max_urls]
        
        log.info(f"scraping {len(urls)} urls...")

        responses = []
        options = {
            "headers": { "content-type": "application/json" }
        }
        if self.proxy:
           options["proxies"] = {
                "https" if "https" in self.proxy else "http": self.proxy
            } 
        
        
        for i, (url, body) in tqdm(enumerate(zip(urls, bodies))):
            time.sleep(1)
            response = requests.post(url, asdict(body), **options)            
            code = response.status_code
            responses.append(response)
            
            if code != 200:
                log.critical(f"request to Skandia yielded {code} response; adding request to backlog for retry")
                url_to_retry = urls[i]
                body_to_retry = bodies[i]
                urls.append(url_to_retry)
                urls.append(body_to_retry)


        print(responses[0].text)
        serialized_data = [
            SkandiaBankenResponse(**r.json(), **asdict(p)) for r, p
            in zip(response, bodies)
        ]
    
        log.info(f"successfully uncpacked {len(responses)}")
        export_df = pd.DataFrame.from_records(asdict(data) for data in serialized_data)
        log.info(f"Successfully scraped {len(export_df)}")
        log.info(f"exporting {self.sinks}")

        for s in self.sinks:
            log.info(f"exporting to {s}")
            s.export(export_df, "skandiabanken")

    def __str__(self):
        return "SkandiaBankenScraper"

    def __repr__(self):
        return str(self)







    








