import os
import subprocess
import pandas as pd

from mortgage_scraper.cli import VERSION
from pathlib import Path


def test_should_find_entrypoint(entrypoint: str):
    assert os.path.isfile(Path(entrypoint)), "cannot locate entrypoint"


def test_should_perform_cli_basics(entrypoint: str):
    result = subprocess.run(["python3", entrypoint, "--version"], capture_output=True)
    assert VERSION in str(result.stdout), "version should be readable"
    assert result.returncode == 0, "should exit without error code"

    result = subprocess.run(["python3", entrypoint, "--help"], capture_output=True)
    assert "Author" in str(result.stdout)
    assert result.returncode == 0, "should exit without error code"


def test_should_run_single_provider_with_limit(entrypoint: str, project_dir: Path):
    data_dir = os.path.join(project_dir, "data")
    files = os.listdir(data_dir)
    result = subprocess.run(
        [
            "python3",
            entrypoint,
            "--target",
            "ica",
            "--sink",
            "csv",
            "--urls-limit",
            "1",
        ]
    )

    assert result.returncode == 0, "should exit without error code"
    added_files = list(set(os.listdir(data_dir)) - set(files))
    assert len(added_files) == 1, "test run did not yield stored csv"
    df = pd.read_csv(os.path.join(data_dir, added_files[0]))
    assert not df.empty, "no data in csv"


def test_should_scrape_in_random_seeded_order(entrypoint: str, project_dir: Path):
    data_dir = os.path.join(project_dir, "data")
    files = os.listdir(data_dir)
    runner = lambda: subprocess.run(
        [
            "python3",
            entrypoint,
            "--target",
            "ica",
            "--sink",
            "csv",
            "--delay",
            "0.5",
            "--urls-limit",
            "1",
            "--randomize-order",
            "--seed",
            "42",
            "--debug",
        ]
    )

    result = runner()

    assert result.returncode == 0, "should exit without error code"
    added_files = list(set(os.listdir(data_dir)) - set(files))
    assert len(added_files) == 1, "test run did not yield stored csv"
    df1 = pd.read_csv(os.path.join(data_dir, added_files[0]))
    assert not df1.empty, "no data in csv"

    result = runner()
    assert result.returncode == 0, "should exit without error code"
    added_files = list(set(os.listdir(data_dir)) - set(files))
    assert len(added_files) == 2, "test run did not yield stored csv"
    df2 = pd.read_csv(os.path.join(data_dir, added_files[1]))
    assert not df2.empty, "no data in csv"

    print(df1)
    print(df2)
    assert (df1.all() == df2.all()).all(), "seeded runs did not equal one another"


def test_should_scrape_hypoteket(entrypoint: str, project_dir: Path):
    data_dir = os.path.join(project_dir, "data")
    files = os.listdir(data_dir)
    result = subprocess.run(
        [
            "python3",
            entrypoint,
            "--target",
            "hypoteket",
            "--sink",
            "csv",
            "--urls-limit",
            "1",
        ]
    )

    assert result.returncode == 0, "should exit without error code"
    added_files = list(set(os.listdir(data_dir)) - set(files))
    assert len(added_files) == 1, "test run did not yield stored csv"
    df = pd.read_csv(os.path.join(data_dir, added_files[0]))
    assert not df.empty, "no data in csv"


def test_should_use_proxy_if_available(entrypoint: str, project_dir: Path):
    if (proxy := os.getenv("PROXY")) is not None:
        data_dir = os.path.join(project_dir, "data")
        files = os.listdir(data_dir)
        result = subprocess.run(
            [
                "python3",
                entrypoint,
                "--target",
                "ica",
                "--sink",
                "csv",
                "--urls-limit",
                "1",
                "--proxy",
                proxy,
            ]
        )

        assert result.returncode == 0, "should exit without error code"
        added_files = list(set(os.listdir(data_dir)) - set(files))
        assert len(added_files) == 1, "test run did not yield stored csv"

        df = pd.read_csv(os.path.join(data_dir, added_files[0]))
        assert not df.empty, "no data in csv"


def test_should_scrape_ica(entrypoint: str, project_dir: Path):
    data_dir = os.path.join(project_dir, "data")
    files = os.listdir(data_dir)
    result = subprocess.run(
        [
            "python3",
            entrypoint,
            "--target",
            "ica",
            "--sink",
            "csv",
            "--urls-limit",
            "1",
        ]
    )

    assert result.returncode == 0, "should exit without error code"
    added_files = list(set(os.listdir(data_dir)) - set(files))
    assert len(added_files) == 1, "test run did not yield stored csv"
    df = pd.read_csv(os.path.join(data_dir, added_files[0]))
    assert not df.empty, "no data in csv"


def test_should_scrape_sbab(entrypoint: str, project_dir: Path):
    data_dir = os.path.join(project_dir, "data")
    files = os.listdir(data_dir)
    result = subprocess.run(
        [
            "python3",
            entrypoint,
            "--target",
            "sbab",
            "--sink",
            "csv",
            "--urls-limit",
            "1",
        ]
    )

    assert result.returncode == 0, "should exit without error code"
    added_files = list(set(os.listdir(data_dir)) - set(files))
    assert len(added_files) == 1, "test run did not yield stored csv"
    df = pd.read_csv(os.path.join(data_dir, added_files[0]))
    assert not df.empty, "no data in csv"


def test_should_scrape_skandia(entrypoint: str, project_dir: Path):
    data_dir = os.path.join(project_dir, "data")
    files = os.listdir(data_dir)
    result = subprocess.run(
        [
            "python3",
            entrypoint,
            "--target",
            "skandia",
            "--sink",
            "csv",
            "--urls-limit",
            "1",
        ]
    )
    assert result.returncode == 0, "should exit without error code"
    added_files = list(set(os.listdir(data_dir)) - set(files))
    assert len(added_files) == 1, "test run did not yield stored csv"
    df = pd.read_csv(os.path.join(data_dir, added_files[0]))
    assert not df.empty, "no data in csv"
