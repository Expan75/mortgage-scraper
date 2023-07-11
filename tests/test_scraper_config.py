import subprocess
import pytest
import numpy as np
from mortgage_scraper.scraper_config import ScraperConfig


def test_should_parse_valid_loan_volume_bin():
    expected_vols = list(np.arange(50_000, 2_000_000, 100_000))
    vols1 = ScraperConfig.parse_loan_volume_bin("[50_000, 2_000_000, 100_000]")
    vols2 = ScraperConfig.parse_loan_volume_bin("[50000,2000000,100000]")
    vols3 = ScraperConfig.parse_loan_volume_bin("50000.0,2000000.0,100000.0")
    assert expected_vols == vols1 == vols2 == vols3


def test_should_reject_invalid_formats():
    with pytest.raises(ValueError):
        ScraperConfig.parse_loan_volume_bin("0.0.0.0.1")
        ScraperConfig.parse_loan_volume_bin("0-100-10")
