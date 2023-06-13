import os
import subprocess
from src.cli import VERSION
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
    result = subprocess.run([
        "python3", entrypoint, 
        "--target", "ica", 
        "--store", "csv", 
        "--limit", "1"
    ])
    assert result.returncode == 0, "should exit without error code"
    assert len(os.listdir(data_dir)) > len(files), "test run did not yield stored csv"


def test_should_use_proxy_if_available(entrypoint: str, project_dir: Path):   
    if (proxy := os.getenv("PROXY")) is not None:
        data_dir = os.path.join(project_dir, "data")
        files = os.listdir(data_dir)
        result = subprocess.run([
            "python3", entrypoint, 
            "--target", "ica", 
            "--store", "csv", 
            "--limit", "1",
            "--proxy", proxy
        ])
        assert result.returncode == 0, "should exit without error code"
        assert len(os.listdir(data_dir)) > len(files), "test run did not store .csv"


def test_should_scrape_hypoteket(entrypoint: str, project_dir: Path):
    data_dir = os.path.join(project_dir, "data")
    files = os.listdir(data_dir)
    result = subprocess.run([
        "python3", entrypoint, 
        "--target", "hypoteket", 
        "--store", "csv", 
        "--limit", "1"
    ])
    assert result.returncode == 0, "should exit without error code"
    assert len(os.listdir(data_dir)) > len(files), "test run did not yield stored csv"



def test_should_scrape_ica(entrypoint: str, project_dir: Path):
    data_dir = os.path.join(project_dir, "data")
    files = os.listdir(data_dir)
    result = subprocess.run([
        "python3", entrypoint, 
        "--target", "ica", 
        "--store", "csv", 
        "--limit", "1"
    ])
    assert result.returncode == 0, "should exit without error code"
    assert len(os.listdir(data_dir)) > len(files), "ica run did not yield stored csv"


def test_should_scrape_sbab(entrypoint: str, project_dir: Path):
    data_dir = os.path.join(project_dir, "data")
    files = os.listdir(data_dir)
    result = subprocess.run([
        "python3", entrypoint, 
        "--target", "sbab", 
        "--store", "csv", 
        "--limit", "1"
    ])
    assert result.returncode == 0, "should exit without error code"
    assert len(os.listdir(data_dir)) > len(files), "test run did not yield stored csv"



def test_should_scrape_skandia(entrypoint: str, project_dir: Path):
    data_dir = os.path.join(project_dir, "data")
    files = os.listdir(data_dir)
    result = subprocess.run([
        "python3", entrypoint, 
        "--target", "skandia", 
        "--store", "csv", 
        "--limit", "1"
    ])
    assert result.returncode == 0, "should exit without error code"
    assert len(os.listdir(data_dir)) > len(files), "test run did not yield stored csv"
