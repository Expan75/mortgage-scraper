"""Fixtures etc."""
import os
import shutil
import pathlib
import pytest
import tempfile
import numpy as np

from mortgage_scraper.csv_sink import CSVSink
from mortgage_scraper.scraper_config import ScraperConfig


@pytest.fixture
def project_dir() -> pathlib.Path:
    return pathlib.Path(os.path.dirname(os.path.realpath(__file__)))


@pytest.fixture()
def data_dir(project_dir) -> str:
    # always clean data dir upon very test run
    path = os.path.join(project_dir, "data")
    shutil.rmtree(path, ignore_errors=True)
    os.makedirs(path, exist_ok=False)

    return path


@pytest.fixture
def entrypoint(project_dir) -> str:
    return os.path.join(project_dir, "main.py")


@pytest.fixture
def temp_dir():
    # github action runner needss speciawl directory
    return os.getenv("RUNNER_TEMP", tempfile.gettempdir())


@pytest.fixture
def default_config():
    return ScraperConfig()


@pytest.fixture
def advanced_config(default_config: ScraperConfig):
    default_config.custom_ltv_granularity = 0.5
    default_config.custom_loan_volume_bins = np.arange(
        50_000, 1_000_000, 50_000
    ).tolist()

    return default_config
