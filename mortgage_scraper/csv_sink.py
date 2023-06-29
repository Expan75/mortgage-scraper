import os
import csv
import logging
import pathlib
from typing import Dict, List, Any, Optional
from datetime import datetime

from mortgage_scraper.base_sink import AbstractSink


log = logging.getLogger(__name__)

Record = Dict[str, Any]
Records = List[Record]


class CSVSink(AbstractSink):
    """
    A sink for exporting scraped data as .csv file
    Exports by provider and multiple providers are meant to share a single sink.
    """

    data_dir = (
        str(pathlib.Path(os.path.dirname(os.path.realpath(__file__))).parent.resolve())
        + "/data"
    )

    def __init__(self, namespace: str):
        self.namespace = str
        self.filepath = self.get_export_filepath(namespace)

        self.f = open(self.filepath, "w+")
        self.writer: Optional[csv.DictWriter] = None

    def write(self, record: Dict):
        if self.writer is None:
            self.writer = csv.DictWriter(self.f, fieldnames=record.keys())
            self.writer.writeheader()
        self.writer.writerow(record)
        self.f.flush()
        log.debug(f"wrote {record} to {self.filepath}")

    def close(self):
        log.debug("export to {self.filepath} done, closing file...")
        self.f.close()

    @classmethod
    def get_export_filepath(cls, namespace) -> str:
        filename = f"{namespace}_mortgage_pricing_" + cls.utc_timestamp() + ".csv"
        return os.path.join(cls.data_dir, filename)

    @staticmethod
    def utc_timestamp() -> str:
        return datetime.now().strftime("%d_%m_%y_%H:%M:%S")

    def __str__(self):
        return "CSVSink"

    def __repr__(self):
        return str(self)
