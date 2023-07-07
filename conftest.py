"""Fixtures etc."""
import os
import shutil
import pathlib
import pytest
import tempfile


from mortgage_scraper.csv_sink import CSVSink


@pytest.fixture
def project_dir() -> pathlib.Path:
    return pathlib.Path(os.path.dirname(os.path.realpath(__file__)))


@pytest.fixture()
def data_dir(project_dir) -> str:
    # always clean data dir upon very test run
    path = os.path.join(project_dir, "data")
    shutil.rmtree(path)
    os.makedirs(path)

    return path


@pytest.fixture
def entrypoint(project_dir) -> str:
    return os.path.join(project_dir, "main.py")


@pytest.fixture
def temp_dir():
    # github action runner needss speciawl directory
    return os.getenv("RUNNER_TEMP", tempfile.gettempdir())
