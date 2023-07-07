import time
import random
import logging
from typing import Any, Dict, List, Optional, Tuple, Union
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict

import requests
from tqdm import tqdm

from mortgage_scraper.base_sink import AbstractSink
from mortgage_scraper.base_scraper import AbstractScraper
from mortgage_scraper.segment import MortgageMarketSegment, generate_segments
from mortgage_scraper.scraper_config import ScraperConfig

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
    base_url = "https://www.icabanken.se/api"

    access_token: str
    token_last_updated_at: datetime

    def __init__(self, sinks: List[AbstractSink], config: ScraperConfig):
        self.sinks = sinks
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({"Content-type": "application/json"})

        if self.config.proxies:
            self.session.proxies.update(self.config.proxies_by_protocol)

        self.refresh_access_token()

    def get_access_token(self) -> str:
        """Retrieves an access token to be used for auth against api"""
        url = self.base_url + "/token/public"
        response = self.session.get(url)
        token_response = AccessTokenResponse(**response.json())
        return token_response.access_token

    def refresh_access_token(self):
        """Util for refreshing access token and saving it"""
        token = self.get_access_token()
        self.token_last_updated_at = datetime.now()
        self.session.headers.update({"Authorization": f"Bearer {token}"})

    @property
    def access_token_expired(self) -> bool:
        token_expiry_date = self.token_last_updated_at + timedelta(minutes=2)
        return datetime.now() > token_expiry_date

    def get_scrape_url(
        self,
        period: Any,
        loan_amount: Union[float, int],
        asset_value: Union[float, int],
    ) -> str:
        print(loan_amount)
        return (
            "https://apimgw-pub.ica.se/t/public.tenant/ica/bank/ac39/mortgage/1.0.0/interestproposal_v2_0?type_of_mortgage=BL"  # noqa
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

        if self.config.randomize_url_order:
            seed = (
                self.config.seed
                if self.config.seed is not None
                else random.randint(1, 1000)
            )
            random.Random(seed).shuffle(segments)

        urls = [
            self.get_scrape_url(s.period, s.loan_amount, s.asset_value)
            for s in segments[: self.config.urls_limit]
        ]
        return urls, segments

    def run_scraping_job(self):
        """Manages the actual scraping job, exporting to each sink and so on"""
        urls, segments = self.generate_scrape_urls()
        log.info(f"scraping {len(urls)} urls...")

        urls_segments_pairs = list(zip(urls, segments))
        for url, segment in tqdm(urls_segments_pairs):
            time.sleep(self.config.delay)
            if self.access_token_expired:
                self.refresh_access_token()

            response = self.session.get(url)

            try:
                parsed = response.json()
                serialized = IcaBankenResponse(**parsed["response"])
                record = {
                    "url": url,
                    "scraped_at": datetime.now(),
                    **asdict(serialized),
                    **asdict(segment),
                }

                for s in self.sinks:
                    s.write(record)

            except requests.exceptions.JSONDecodeError:
                log.critical(f"could not parse json, skipping {url=}")

        log.info("finished scrape job")
        for s in self.sinks:
            s.close()

    def __str__(self):
        return "IcaBankenScraper"

    def __repr__(self):
        return str(self)
