import os
import shutil
from unittest.mock import patch
from mortgage_scraper.csv_sink import CSVSink


def test_export(temp_dir):
    with patch("mortgage_scraper.csv_sink.CSVSink.data_dir", temp_dir):
        n_files_before = len(os.listdir(temp_dir))
        sink = CSVSink(namespace="test", ts_format="%Y-%m-%d-%H:%M:%S")
        sink.write({"point": 42})
        n_files_after = len(os.listdir(temp_dir))
        assert n_files_after == n_files_before + 1, "no csv was added"


def test_should_handle_not_existing_directories(temp_dir: str):
    nested_dir = os.path.join(temp_dir, "data")
    try:
        shutil.rmtree(nested_dir, ignore_errors=False)
    except FileNotFoundError:
        pass

    with patch("mortgage_scraper.csv_sink.CSVSink.data_dir", nested_dir):
        assert not os.path.isdir(nested_dir) and not os.path.exists(nested_dir)
        sink = CSVSink(namespace="test", ts_format="%Y-%m-%d-%H:%M:%S")
        sink.write({"point": 42})

        assert os.path.isdir(nested_dir) and os.path.exists(nested_dir)
        n_files_after = len([f for f in os.listdir(nested_dir) if len(f) > 2])
        assert n_files_after == 1, "no csv was added"
