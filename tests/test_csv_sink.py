import os
import sys
import tempfile
import pytest
import pandas as pd
from src.csv_sink import CSVSink


def test_sink_repr(csv_sink: CSVSink):
    assert str(csv_sink) == "CSVSink"


def test_export(csv_sink: CSVSink, temp_dir):

    csv_sink.data_dir = temp_dir
    csv_sink.export(pd.DataFrame(), name="somename")

    assert len(os.listdir(temp_dir)) > 0
    assert [filename for filename in os.listdir(temp_dir) if ".csv" in filename]