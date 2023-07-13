import os
import subprocess
from datetime import datetime
import pandas as pd
from pandas._libs.lib import is_datetime64_array
from pandas.api.types import (
    is_integer_dtype,
    is_float_dtype,
    is_numeric_dtype,
    is_string_dtype,
    is_datetime64_any_dtype,
)
from pandas.testing import assert_frame_equal
from functools import cmp_to_key
from mortgage_scraper.cli import VERSION

DEFAULT_TS_FORMAT = "%Y-%m-%d-%H-%M-%S"
EXPECTED_COLUMN_TYPES = {
    "ltv": is_float_dtype,
    "period": is_integer_dtype,
    "bank": is_string_dtype,
    "asset_value": is_numeric_dtype,
    "loan_amount": is_numeric_dtype,
    "scraped_at": lambda col: is_datetime64_any_dtype(
        pd.to_datetime(col, format=DEFAULT_TS_FORMAT)
    ),
}


def column_types_match_expected(df):
    for col, expected_type_f in EXPECTED_COLUMN_TYPES.items():
        assert expected_type_f(
            df[col]
        ), f"{df[col].dtype} does not match expected {expected_type_f=}"
    return True


def parse_timestamp(filename: str) -> datetime:
    return datetime.strptime(filename[-23:-4], DEFAULT_TS_FORMAT)


def get_latest_csv_dump(data_dir: str) -> pd.DataFrame:
    files = [f for f in os.listdir(data_dir) if ".csv" in f]
    latest = lambda f1, f2: parse_timestamp(f1) > parse_timestamp(f2)  # noqa
    files_most_recent = list(sorted(files, key=cmp_to_key(latest), reverse=True))
    filepath_most_recent = os.path.join(data_dir, files_most_recent[0])

    return pd.read_csv(filepath_most_recent)


def test_should_perform_cli_basics():
    result = subprocess.run(
        ["python3", "-m", "mortgage_scraper", "--version"], capture_output=True
    )
    assert VERSION in str(result.stdout), "version should be readable"
    assert result.returncode == 0, "should exit without error code"

    result = subprocess.run(
        ["python3", "-m", "mortgage_scraper", "--help"], capture_output=True
    )
    assert "Author" in str(result.stdout)
    assert result.returncode == 0, "should exit without error code"


def test_should_run_single_provider_with_limit(data_dir: str):
    files = os.listdir(data_dir)
    result = subprocess.run(
        [
            "python3",
            "-m",
            "mortgage_scraper",
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


def test_random_order_should_mean_mixed_segments(data_dir: str):
    files = os.listdir(data_dir)
    result = subprocess.run(  # noqa
        [
            "python3",
            "-m",
            "mortgage_scraper",
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


def test_should_scrape_in_random_seeded_order(data_dir: str):
    files = os.listdir(data_dir)
    runner = lambda: subprocess.run(  # noqa
        [
            "python3",
            "-m",
            "mortgage_scraper",
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


def test_should_scrape_hypoteket(data_dir: str):
    files = os.listdir(data_dir)
    result = subprocess.run(
        [
            "python3",
            "-m",
            "mortgage_scraper",
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
    assert not df.period.isna().values.any(), "period was not attached succesfully"
    assert not df.empty, "no data in csv"
    assert column_types_match_expected(df), f"column types do not match for {df.info()}"


def test_should_use_proxy_if_available(data_dir: str):
    if (proxy := os.getenv("PROXY")) is not None:
        files = os.listdir(data_dir)
        result = subprocess.run(
            [
                "python3",
                "-m",
                "mortgage_scraper",
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
        assert not df.period.isna().values.any(), "period was not attached succesfully"
        assert column_types_match_expected(
            df
        ), f"column types do not match for {df.info()}"


def test_should_scrape_ica(data_dir: str):
    files = os.listdir(data_dir)
    result = subprocess.run(
        [
            "python3",
            "-m",
            "mortgage_scraper",
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
    assert not df.period.isna().values.any(), "period was not attached succesfully"
    assert column_types_match_expected(df), f"column types do not match for {df.info()}"
    assert not df.empty, "no data in csv"


def test_should_scrape_sbab(data_dir: str):
    files = os.listdir(data_dir)
    result = subprocess.run(
        [
            "python3",
            "-m",
            "mortgage_scraper",
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
    assert not df.period.isna().values.any(), "period was not attached succesfully"
    assert column_types_match_expected(df), f"column types do not match for {df.info()}"
    assert not df.empty, "no data in csv"


def test_should_scrape_skandia(data_dir: str):
    files = os.listdir(data_dir)
    result = subprocess.run(
        [
            "python3",
            "-m",
            "mortgage_scraper",
            "--target",
            "skandia",
            "--sink",
            "csv",
            "--urls-limit",
            "1",
            "--rotate-user-agent",
            "--randomize",
            "--seed",
            "42",
        ]
    )
    assert result.returncode == 0, "should exit without error code"
    added_files = list(set(os.listdir(data_dir)) - set(files))
    assert len(added_files) == 1, "test run did not yield stored csv"
    df = pd.read_csv(os.path.join(data_dir, added_files[0]))
    assert not df.period.isna().values.any(), "period was not attached succesfully"
    assert column_types_match_expected(df), f"column types do not match for {df.info()}"
    assert not df.empty, "no data in csv"
