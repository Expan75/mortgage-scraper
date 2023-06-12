import os
import logging
import pathlib
import pandas as pd

from datetime import datetime
from src.base_sink import AbstractSink


log = logging.getLogger(__file__)
logging.basicConfig(level=logging.INFO)


class CSVSink(AbstractSink):
    """A sink for exporting scraped data as .csv file"""

    data_dir = str(pathlib.Path(
        os.path.dirname(os.path.realpath(__file__))
    ).parent.resolve()) + "/data"

    def export(self, df: pd.DataFrame, name: str):
        """Expects a named df"""
        filepath = self.get_export_filepath(name)
        log.info("saving to %s", filepath)
        df.to_csv(filepath)

    def get_export_filepath(self, name) -> str:
        return os.path.join(self.data_dir, self.get_timestamped_filename(name))

    def get_timestamped_filename(self, name: str = "competitor") -> str:
        return f"{name}_mortgage_pricing_" + self.utc_timestamp() + ".csv"

    def utc_timestamp(self) -> str:
        return datetime.now().strftime("%d_%m_%y_%H:%M:%S")

    def __str__(self):
        return "CSVSink"

    def __repr__(self):
        return str(self)




