import random
from typing import Optional, List, Dict, Union

from numpy._typing import _16Bit
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
    user_agents: Optional[List[str]] = Field(default_factory=list)

    # route requests via proxy, if multiple are given, uses round robin
    proxies: Optional[List[str]] = Field(default_factory=list)

    # fed into sinks and scraped datapoints
    ts_format: str = "%Y-%m-%d-%H:%M:%S"

    def __post_init__(self):
        if self.rotate_user_agent:
            with open(self.user_agents_filepath, "r") as f:
                self.user_agents = [line for line in f if len(line) > 10]

    def get_random_user_agent_header(self) -> Union[Dict, Dict[str, str]]:
        if self.user_agents:
            return {"User-Agent": random.choice(self.user_agents)}
        return {}

    @property
    def proxies_by_protocol(self) -> Union[Dict[str, str], Dict]:
        proxies: Dict[str, str] = {}
        if self.proxies is not None:
            for proxy in self.proxies:
                protocol = "https" if "https" in proxy else "http"
                proxies[protocol] = proxy
        return proxies

    @property
    def proxy(self) -> bool:
        return self.proxies is not None and len(self.proxies) > 0
