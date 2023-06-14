from dataclasses import dataclass
from abc import ABC, abstractmethod


@dataclass
class ScraperConfig:
    proxy: str


class AbstractScraper(ABC):

    @abstractmethod
    def run_scraping_job(self):
        pass

    @abstractmethod
    def __str__(self):
        pass
