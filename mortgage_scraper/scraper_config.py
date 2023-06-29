from typing import Union, List, Dict, Optional
from pydantic.dataclasses import dataclass



@dataclass
class ScraperConfig():

    # https://someproxy.com
    proxy: str = ""

    # caps the number of urls we'd like to scrape
    max_urls: Optional[int] = None

    # misc
    debug: bool = False
