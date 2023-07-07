import sys
import logging
import argparse
from typing import List, Dict, Set, Any
from mortgage_scraper.base_scraper import AbstractScraper
from mortgage_scraper.csv_sink import CSVSink
from mortgage_scraper.ica_scraper import IcaBankenScraper
from mortgage_scraper.hypoteket_scraper import HypoteketScraper
from mortgage_scraper.sbab_banken_scraper import SBABScraper
from mortgage_scraper.skandia_scraper import SkandiaBankenScraper
from mortgage_scraper.scraper_config import ScraperConfig


IMPLEMENTED_SCRAPERS: Dict[str, Any] = {
    "sbab": SBABScraper,
    "ica": IcaBankenScraper,
    "hypoteket": HypoteketScraper,
    "skandia": SkandiaBankenScraper,
}

IMPLEMENTED_SINKS = {
    "csv": CSVSink,
}


INVALID_SINK_MESSAGE = f"""
    Please provide one or many valid sinks out of: {list(IMPLEMENTED_SINKS.keys())}

"""

INVALID_SCRAPER_MESSAGE = f"""
    Please provide one or many valid scrapers out of {list(IMPLEMENTED_SCRAPERS.keys())}
"""

__all__ = ["cli"]

VERSION = "Mortgage Scraper v1.0.0"


def setup_loggers(debug: bool):
    """Helper for setting up logging with adjustable debug level"""

    level = logging.DEBUG if debug else logging.INFO

    logger = logging.getLogger()
    logger.setLevel(level)
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(level)
    stdout_handler.setFormatter(formatter)

    file_handler = logging.FileHandler("mortgage_scraper.log")
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stdout_handler)


log = logging.getLogger(__name__)


def cli():
    parser = argparse.ArgumentParser(
        prog="Mortgage Scraper",
        description="Scrapes mortgage providers for their pricing structure",
        epilog="Author: Erik Håkansson",
    )

    # core
    parser.add_argument("-t", "--target", nargs="*", required=True, type=str)
    parser.add_argument("-s", "--sink", nargs="*", required=True, type=str)

    # adv. settings
    parser.add_argument("-l", "--rate-limit", default=None, type=int)
    parser.add_argument("-u", "--urls-limit", default=None, type=int)
    parser.add_argument("-p", "--proxies", nargs="*", type=str)
    parser.add_argument("-w", "--delay", default=0.0, type=float)

    parser.add_argument("-r", "--randomize", action="store_true", default=False)
    parser.add_argument("-a", "--rotate-user-agent", action="store_true", default=False)
    parser.add_argument("-e", "--seed", default=42, type=int)

    # misc. cli
    parser.add_argument("-v", "--version", action="version", version=VERSION)
    parser.add_argument("-d", "--debug", action="store_true", default=False)

    args = parser.parse_args()

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
    setup_loggers(args.debug)

    config = ScraperConfig(
        debug=args.debug,
        delay=args.delay,
        rate_limit=args.rate_limit,
        urls_limit=args.urls_limit,
        randomize_url_order=args.randomize,
        randomize_url_seed=args.seed,
        proxies=args.proxies,
        rotate_user_agent=args.rotate_user_agent,
    )
    selected_sinks = find_matching_sinks(args.sink)
    selected_scrapers = find_matching_scrapers(args.target)

    scrapers = setup_scrapers(selected_scrapers, selected_sinks, config)

    log.info(f"Selected data sinks: {selected_sinks}")
    log.info(f"Selected scraping targets: {selected_scrapers}")
    log.info("Beginning scraping job...")

    for scraper in scrapers:
        scraper.run_scraping_job()

    log.info("Completed jobs, exiting...")
    return True
