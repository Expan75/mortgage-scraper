"""Fixtures etc."""
import os
import pathlib
import pytest
import tempfile


from mortgage_scraper.csv_sink import CSVSink


@pytest.fixture
def project_dir() -> pathlib.Path:
    return pathlib.Path(os.path.dirname(os.path.realpath(__file__)))


@pytest.fixture
def entrypoint(project_dir):
    return os.path.join(project_dir, "main.py")


@pytest.fixture
def temp_dir():
    # github action runner needss speciawl directory
    return os.getenv("RUNNER_TEMP", tempfile.gettempdir())


@pytest.fixture
def csv_sink():
    return CSVSink(namespace="test")
