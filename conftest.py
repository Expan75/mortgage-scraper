"""Fixtures etc."""
import tempfile
import pytest
from src.csv_sink import CSVSink


@pytest.fixture
def temp_dir():
    return tempfile.gettempdir()


@pytest.fixture
def csv_sink():
    return CSVSink()
