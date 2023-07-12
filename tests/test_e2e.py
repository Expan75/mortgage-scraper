import os
import subprocess
from datetime import datetime
import pandas as pd
from pandas.testing import assert_frame_equal
from functools import cmp_to_key
from mortgage_scraper.cli import VERSION
from pathlib import Path

DEFAULT_TS_FORMAT = "%Y-%m-%d-%H-%M-%S"


def parse_timestamp(filename: str) -> datetime:
    return datetime.strptime(filename[-23:-4], DEFAULT_TS_FORMAT)


def get_latest_csv_dump(data_dir: str) -> pd.DataFrame:
    files = [f for f in os.listdir(data_dir) if ".csv" in f]
    latest = lambda f1, f2: parse_timestamp(f1) > parse_timestamp(f2)  # noqa
    files_most_recent = list(sorted(files, key=cmp_to_key(latest), reverse=True))
    filepath_most_recent = os.path.join(data_dir, files_most_recent[0])

    return pd.read_csv(filepath_most_recent)


def test_should_find_entrypoint(entrypoint: str):
    assert os.path.isfile(Path(entrypoint)), "cannot locate entrypoint"


def test_should_perform_cli_basics(entrypoint: str):
    result = subprocess.run(["python3", entrypoint, "--version"], capture_output=True)
    assert VERSION in str(result.stdout), "version should be readable"
    assert result.returncode == 0, "should exit without error code"

    result = subprocess.run(["python3", entrypoint, "--help"], capture_output=True)
    assert "Author" in str(result.stdout)
    assert result.returncode == 0, "should exit without error code"


def test_should_run_single_provider_with_limit(entrypoint: str, data_dir: str):
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
    df = get_latest_csv_dump(data_dir)
    assert not df.empty, "no data in csv"


def test_random_order_should_mean_mixed_segments(entrypoint: str, data_dir: str):
    files = os.listdir(data_dir)
    result = subprocess.run(  # noqa
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
            "25",
            "--randomize",
            "--seed",
            "42",
            "--debug",
        ]
    )
    added_files = list(set(os.listdir(data_dir)) - set(files))
    df = get_latest_csv_dump(data_dir)

    assert result.returncode == 0, "should exit without error code"
    assert len(added_files) == 1, "test run did not yield stored csv"
    assert not df.empty, "no data in csv"
    assert len(df.loan_amount.unique()) > 1, "There should be different loan amounts"
    assert len(df.asset_value.unique()) > 1, "There should be different asset values"


def test_should_scrape_in_random_seeded_order(entrypoint: str, data_dir: str):
    files = os.listdir(data_dir)
    runner = lambda: subprocess.run(  # noqa
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
            "--randomize",
            "--seed",
            "42",
            "--debug",
        ]
    )

    result = runner()

    assert result.returncode == 0, "should exit without error code"
    added_files = list(set(os.listdir(data_dir)) - set(files))
    assert len(added_files) == 1, "test run did not yield stored csv"
    df1 = get_latest_csv_dump(data_dir)
    assert not df1.empty, "no data in csv"

    result = runner()
    assert result.returncode == 0, "should exit without error code"
    added_files = list(set(os.listdir(data_dir)) - set(files))
    assert len(added_files) == 2, "test run did not yield stored csv"
    df2 = get_latest_csv_dump(data_dir)
    assert not df2.empty, "no data in csv"
    assert df1 is not df2

    changing_columns = {"scraped_at", "ltv", "asset_value", "loan_amount"}
    fixed_cols = [c for c in df1.columns if c not in changing_columns]
    assert_frame_equal(
        df1[fixed_cols], df2[fixed_cols]
    ), "seeded runs did not equal one another"


def test_should_scrape_hypoteket(entrypoint: str, data_dir: str):
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


def test_should_use_proxy_if_available(entrypoint: str, data_dir: str):
    if (proxy := os.getenv("PROXY")) is not None:
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


def test_should_scrape_ica(entrypoint: str, data_dir: str):
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


def test_should_scrape_sbab(entrypoint: str, data_dir: str):
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


def test_should_scrape_skandia(entrypoint: str, data_dir: str):
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
