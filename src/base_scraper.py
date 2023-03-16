from abc import ABC, abstractmethod

class AbstractScraper(ABC):

    @abstractmethod
    def run_scraping_job():
        pass

    @abstractmethod
    def __str__():
        pass
