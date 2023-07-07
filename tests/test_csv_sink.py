import os
from unittest.mock import patch
from mortgage_scraper.csv_sink import CSVSink


def test_export(temp_dir):
    with patch("mortgage_scraper.csv_sink.CSVSink.data_dir", temp_dir):
        n_files_before = len(os.listdir(temp_dir))
        sink = CSVSink(namespace="test", ts_format="%Y-%m-%d-%H:%M:%S")
        sink.write({"point": 42})
        n_files_after = len(os.listdir(temp_dir))
        assert n_files_after == n_files_before + 1, "no csv was added"
