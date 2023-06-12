from abc import ABC, abstractmethod

class AbstractScraper(ABC):

    @abstractmethod
    def run_scraping_job(self):
        pass

    @abstractmethod
    def __str__(self):
        pass
