import logging
import argparse
from typing import List, Dict, Optional, Union, Any
from src.base_sink import AbstractSink
from src.base_scraper import AbstractScraper
from src.csv_sink import CSVSink
from src.ica_scraper import IcaBankenScraper
from src.hypoteket_scraper import HypoteketScraper
from src.sbab_banken_scraper import SBABScraper
from src.skandia_scraper import SkandiaBankenScraper


log = logging.getLogger(__name__)
logging.basicConfig(filename="mortgage_scraper.log",
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)

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


def setup_sinks(selected_sinks: List[str]) -> List[AbstractSink]:
    """Creates the neccessary objects following CLI options"""
    matching_sinks = set(selected_sinks) & set(IMPLEMENTED_SINKS.keys())
    assert len(matching_sinks) > 0, INVALID_SINK_MESSAGE
    return [IMPLEMENTED_SINKS[s]() for s in matching_sinks]


def setup_scrapers(
    sinks: List[AbstractSink], 
    selected_targets: List[str],
    max_urls: Optional[int],
    proxy: str
) -> List[AbstractScraper]:
    """Setups the selected scrapers based off of config"""
    matching_targets = set(selected_targets) & set(IMPLEMENTED_SCRAPERS.keys())
    assert len(matching_targets) > 0, INVALID_SCRAPER_MESSAGE
    return [
        IMPLEMENTED_SCRAPERS[t](
            sinks=sinks,
            max_urls=max_urls, 
            proxy=proxy
        ) 
        for t in matching_targets
    ]


def main():
    """Main Entrypoint of scraper CLI tool"""

    args = cli()
    if args.debug is True:
        logging.basicConfig(level=logging.DEBUG)

    initalised_sinks = setup_sinks(selected_sinks=args.sinks)
    initalised_scrapers = setup_scrapers(
        sinks=initalised_sinks, 
        selected_targets=args.targets,
        max_urls=args.limit,
        proxy=args.proxy
    )

    log.info(f"Selected data sinks: {initalised_sinks}")
    log.info(f"Selected scraping targets: {initalised_scrapers}")
    log.info("Beginning scraping job...")

    for scraper in initalised_scrapers:
        scraper.run_scraping_job()

    log.info("Completed jobs, exiting...")
    return True
