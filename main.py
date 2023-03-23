import logging
import argparse
from typing import List, Tuple, Dict

from src.base_sink import AbstractSink
from src.base_scraper import AbstractScraper
from src.csv_sink import CSVSink
from src.ica_scraper import IcaBankenScraper
from src.hypoteket_scraper import HypoteketScraper
from src.sbab_banken_scraper import SBABScraper


log = logging.getLogger(__file__)
logging.basicConfig(level=logging.INFO)


IMPLEMENTED_SINKS = {
    "csv": CSVSink,
}

IMPLEMENTED_SCRAPERS = {
    "sbab": SBABScraper,
    "ica": IcaBankenScraper,
    "hypoteket": HypoteketScraper,
}


def cli():
    parser = argparse.ArgumentParser(
        prog = "Mortgage Scraper",
        description = "Scrapes mortgage providers for their pricing structure",
        epilog = "Author: Erik Håkansson",
    )

    parser.add_argument("-t", "--target", required=True)
    parser.add_argument("-s", "--store", required=True)
    parser.add_argument("-d", "--debug", action="store_true")
    args = parser.parse_args()
    options = {
        "targets": [t.lower().strip() for t in args.target.split(",")],
        "sinks": [t.lower().strip() for t in args.store.split(",")],
        "debug": args.debug
    }
    return options


def setup_sinks(selected_sinks: List[str]) -> List[AbstractSink]:
    """Creates the neccessary objects following CLI options"""
    matching_sinks = set(selected_sinks) & set(IMPLEMENTED_SINKS.keys())
    assert len(matching_sinks) > 0, f"Please provide a valid list of data sinks; should be one of {list(IMPLEMENTED_SINKS.keys())}"
    return [IMPLEMENTED_SINKS[s]() for s in matching_sinks]


def setup_scrapers(sinks: List[AbstractSink], selected_targets: List[str]) -> List[AbstractScraper]:
    """Setups the selected scrapers based off of config"""
    matching_targets = set(selected_targets) & set(IMPLEMENTED_SCRAPERS.keys())
    assert len(matching_targets) > 0, f"Please provide a valid list of api targets; should be one of {list(IMPLEMENTED_SCRAPERS.keys())}"
    return [IMPLEMENTED_SCRAPERS[t](sinks=sinks) for t in matching_targets]


def main():

    # parse
    targets, sinks, debug = cli().values()
    
    max_urls = float("inf")

    if debug:
        logging.basicConfig(level=logging.DEBUG)
        max_urls = 10

    # intialise
    initalised_sinks = setup_sinks(selected_sinks=sinks)
    initalised_scrapers = setup_scrapers(sinks=initalised_sinks, selected_targets=targets)

    # run and log
    log.info(f"Selected data sinks: {initalised_sinks}")
    log.info(f"Selected scraping targets: {initalised_scrapers}")
    log.info("Beginning scraping job...")

    for scraper in initalised_scrapers:
        scraper.run_scraping_job(max_urls)

    log.info("Completed jobs, exiting...")
    return True


if __name__ == "__main__":
    main()






