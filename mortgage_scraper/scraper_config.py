import os
import logging
import pathlib
import random
from typing import Optional, List, Dict, Union

import numpy as np
from pydantic import Field
from pydantic.dataclasses import dataclass


log = logging.getLogger(__name__)


@dataclass
class ScraperConfig:
    # more extensive logging etc.
    debug: bool = False

    # sleep between requests
    delay: float = 0

    # custom loan volume bins
    custom_ltv_granularity: Optional[float] = 0.01
    custom_loan_volume_bins: Optional[List[int]] = Field(default_factory=list)

    # intepretad as requests/second
    rate_limit: Optional[int] = None

    # cap urls, useful for debugging
    urls_limit: Optional[int] = None

    # send generated urls in a seeded randomised order
    randomize_url_order: bool = False
    seed: int = 42

    # switches attached user agent in headers with provided List
    rotate_user_agent: bool = False
    user_agents_filepath: str = os.path.join(
        pathlib.Path(__file__).resolve().parent.parent, "agents.txt"
    )

    # route requests via proxy, if multiple are given, uses round robin
    proxies: Optional[List[str]] = Field(default_factory=list)

    # fed into sinks and scraped datapoints
    ts_format: str = "%Y-%m-%d-%H-%M-%S"

    def __post_init__(self):
        self.user_agents = self.load_user_agents()

    def get_random_user_agent_header(self) -> Dict[str, str]:
        return {"User-Agent": random.choice(self.user_agents)}

    @classmethod
    def load_user_agents(cls) -> List[str]:
        with open(cls.user_agents_filepath, "rb+") as f:
            return [line.rstrip().decode("utf-8") for line in f.readlines()]

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

    @staticmethod
    def parse_loan_volume_bin(raw_input: str) -> List[int]:
        """
        Parses a loan volume bin provided as a raw CLI input argument
        Expects an internval given as: [interval_start,interval_end,interval_step]
        """
        cleaned_input = raw_input.replace("[", "").replace("]", "").replace(" ", "")
        try:
            start, end, step = [int(float(n)) for n in cleaned_input.split(",")]
            return [int(v) for v in np.arange(start, end, step)]
        except ValueError as e:
            print(e)
            raise ValueError(f"{raw_input=} is not a parseable loan volume bin")
