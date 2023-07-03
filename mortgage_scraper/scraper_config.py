from typing import Optional, Union, List, Dict
from pydantic.dataclasses import dataclass


@dataclass
class ScraperConfig:
    # more extensive logging etc.
    debug: bool = False

    # sleep between requests
    delay: Optional[Union[int, float]] = None

    # intepretad as requests/second
    rate_limit: Optional[int] = None

    # cap urls, useful for debugging
    urls_limit: Optional[int] = None

    # send generated urls in a seeded randomised order
    randomise_url_order: bool = False
    randomise_url_seed: int = 42

    # switches attached user agent in headers with provided List
    rotate_user_agent: bool = False
    user_agents_filepath: str = "../agents.txt"

    # route requests via proxy
    proxies: List[str] = []

    @property
    def proxies_by_protocol(self) -> Dict[str, List[str]]:
        proxies: Dict[str, List[str]] = {"http": [], "https": []}
        for proxy in self.proxies:
            protocol = "https" if "https" in proxy else "http"
            proxies[protocol].append(proxy)
        return proxies

    @property
    def proxy(self) -> bool:
        return len(self.proxies) > 0
