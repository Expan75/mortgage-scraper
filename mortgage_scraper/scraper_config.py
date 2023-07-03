from typing import Optional
from pydantic.dataclasses import dataclass


@dataclass
class ScraperConfig:
    # https://someproxy.com
    proxy: str = ""

    # caps the number of urls we'd like to scrape
    max_urls: Optional[int] = None

    # send off urls in random order
    permutate: bool = False

    # misc
    debug: bool = False
