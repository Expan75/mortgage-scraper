from abc import ABC, abstractmethod
from mortgage_scraper.scraper_config import ScraperConfig
from mortgage_scraper.base_sink import AbstractSink


class AbstractScraper(ABC):
    @abstractmethod
    def __init__(self, sinks: AbstractSink, config: ScraperConfig):
        pass

    @abstractmethod
    def run_scraping_job(self):
        pass

    @abstractmethod
    def __str__(self):
        pass
