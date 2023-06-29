import sys
import logging
import argparse
from typing import List, Dict, Optional, Union, Set, Any
from src.base_sink import AbstractSink
from src.base_scraper import AbstractScraper
from src.csv_sink import CSVSink
from src.ica_scraper import IcaBankenScraper
from src.hypoteket_scraper import HypoteketScraper
from src.sbab_banken_scraper import SBABScraper
from src.skandia_scraper import SkandiaBankenScraper
from src.scraper_config import ScraperConfig

logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')

stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setLevel(logging.DEBUG)
stdout_handler.setFormatter(formatter)

file_handler = logging.FileHandler('mortgage_scraper.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)


logger.addHandler(file_handler)
logger.addHandler(stdout_handler)

log = logging.getLogger(__name__)


__all__ = ["cli"]

VERSION = "Mortgage Scraper v1.0.0"

IMPLEMENTED_SINKS = {
    "csv": CSVSink,
}

IMPLEMENTED_SCRAPERS: Dict[str, Any] = {
    "sbab": SBABScraper,
    "ica": IcaBankenScraper,
    "hypoteket": HypoteketScraper,
    "skandia": SkandiaBankenScraper,
}

INVALID_SINK_MESSAGE = f"""
    Please provide one or many valid sinks out of: {list(IMPLEMENTED_SINKS.keys())}
"""

INVALID_SCRAPER_MESSAGE = f"""
    Please provide one or many valid scrapers out of {list(IMPLEMENTED_SCRAPERS.keys())}
"""



def cli():
    parser = argparse.ArgumentParser(
        prog = "Mortgage Scraper",
        description = "Scrapes mortgage providers for their pricing structure",
        epilog = "Author: Erik Håkansson",
    )
    parser.add_argument("-t", "--target", required=True)
    parser.add_argument("-s", "--store", required=True)
    parser.add_argument("-d", "--debug", action="store_true", default=False)
    parser.add_argument("-v", "--version", action="version", version=VERSION)
    parser.add_argument("-l", "--limit", default=None, type=int) 
    parser.add_argument("-p", "--proxy", default="", type=str)
    args = parser.parse_args()
    args.targets = [t.lower().strip() for t in args.target.split(",")]
    args.sinks = [t.lower().strip() for t in args.store.split(",")]
 
    return args


def find_matching_sinks(selected_sinks: List[str]) -> Set[str]:
    matching_sinks = set(selected_sinks) & set(IMPLEMENTED_SINKS.keys())
    assert len(matching_sinks) > 0, INVALID_SINK_MESSAGE
    return matching_sinks


def find_matching_scrapers(selected_targets: List[str]) -> Set[str]:
    matching_targets = set(selected_targets) & set(IMPLEMENTED_SCRAPERS.keys())
    assert len(matching_targets) > 0, INVALID_SCRAPER_MESSAGE
    return matching_targets


def setup_scraper(
        scraper: str, sinks: List[str], config: ScraperConfig
) -> AbstractScraper:
    log.info(f"settings sinks with namespace: {scraper}")
    scraper_sinks = [IMPLEMENTED_SINKS[s](namespace=scraper) for s in sinks]
    return IMPLEMENTED_SCRAPERS[scraper](scraper_sinks, config)


def setup_scrapers(
        selected_scrapers: List[str],
        selected_sinks: List[str],
        config: ScraperConfig,
    ) -> List[AbstractScraper]:
    """Creates ready to go scraper objects"""
    return [setup_scraper(s, selected_sinks, config) for s in selected_scrapers]
     

def main():
    """Main Entrypoint of scraper CLI tool"""

    args = cli()
    if args.debug is True:
        logging.basicConfig(level=logging.DEBUG)
    
    config = ScraperConfig(debug=args.debug, max_urls=args.limit, proxy=args.proxy)
    selected_sinks = find_matching_sinks(args.sinks)
    selected_scrapers = find_matching_scrapers(args.targets)

    scrapers = setup_scrapers(selected_scrapers, selected_sinks, config)

    log.info(f"Selected data sinks: {selected_sinks}")
    log.info(f"Selected scraping targets: {selected_scrapers}")
    log.info("Beginning scraping job...")

    for scraper in scrapers:
        scraper.run_scraping_job()

    log.info("Completed jobs, exiting...")
    return True
