from typing import Optional, List, Dict
from pydantic import Field
from pydantic.dataclasses import dataclass


@dataclass
class ScraperConfig:
    # more extensive logging etc.
    debug: bool = False

    # sleep between requests
    delay: float = 0

    # intepretad as requests/second
    rate_limit: Optional[int] = None

    # cap urls, useful for debugging
    urls_limit: Optional[int] = None

    # send generated urls in a seeded randomised order
    randomize_url_order: bool = False
    seed: int = 42

    # switches attached user agent in headers with provided List
    rotate_user_agent: bool = False
    user_agents_filepath: str = "../agents.txt"

    # route requests via proxy, if multiple are given, uses round robin
    proxies: Optional[List[str]] = Field(default_factory=list)

    @property
    def proxies_by_protocol(self) -> Dict[str, str]:
        proxies: Dict[str, str] = {}
        for proxy in self.proxies:
            protocol = "https" if "https" in proxy else "http"
            proxies[protocol] = proxy
        return proxies

    @property
    def proxy(self) -> bool:
        return len(self.proxies) > 0
