import os
import csv
import logging
import pathlib
from typing import Dict, List, Any, Optional
from datetime import datetime

from mortgage_scraper.base_sink import AbstractSink
from mortgage_scraper.scraper_config import ScraperConfig


log = logging.getLogger(__name__)

Record = Dict[str, Any]
Records = List[Record]


class CSVSink(AbstractSink):
    """
    A sink for exporting scraped data as .csv file
    Exports by provider and multiple providers are meant to share a single sink.
    """

    project_dir = pathlib.Path(
        os.path.dirname(os.path.realpath(__file__))
    ).parent.resolve()
    data_dir = os.path.join(project_dir, "data")

    # columsn that will be given columns without being compressed
    # into a "raw" json payload
    CORE_COLUMNS = set(
        [
            "url",
            "offered_interest_rate",
            "asset_value",
            "loan_amount",
            "period",
            "ltv",
            "bank",
            "scraped_at",
        ]
    )

    def __init__(self, namespace: str, config: ScraperConfig):
        os.makedirs(self.data_dir, exist_ok=True)

        self.namespace = namespace
        self.config: ScraperConfig = config
        self.filepath = self.get_export_filepath(namespace, config.ts_format)

        self.f = open(self.filepath, "w+")
        self.writer: Optional[csv.DictWriter] = None

    def write(self, record: Dict):
        # attach meta data
        record["scraped_at"] = datetime.now().strftime(self.config.ts_format)
        record["bank"] = self.namespace

        # compress non-core columns into raw json string
        record_core = {k: v for k, v in record.items() if k in self.CORE_COLUMNS}
        record_aux_fields = {k: v for k, v in record.items() if k not in record_core}
        adjusted_record = {**record_core, "json": record_aux_fields}

        if self.writer is None:
            self.writer = csv.DictWriter(
                self.f,
                fieldnames=adjusted_record.keys(),
                quoting=csv.QUOTE_MINIMAL,
            )
            self.writer.writeheader()

        self.writer.writerow(adjusted_record)
        self.f.flush()
        log.debug(f"wrote {record} to {self.filepath}")

    def close(self):
        log.info(f"export to {self.filepath} done, closing file...")
        self.f.close()

    @classmethod
    def get_export_filepath(cls, namespace: str, ts_format: str) -> str:
        filename = (
            f"{namespace}_mortgage_pricing_"
            + datetime.now().strftime(ts_format)
            + ".csv"
        )
        return os.path.join(cls.data_dir, filename)

    def __str__(self):
        return "CSVSink"

    def __repr__(self):
        return str(self)
