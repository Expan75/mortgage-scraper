import pytest
import requests
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


def test_should_get_random_user_agent(advanced_config: ScraperConfig):
    agent_header = advanced_config.get_random_user_agent_header()
    assert agent_header, "no or empty agent header"

    # ensure header is attachable
    s = requests.session()
    s.headers.update(agent_header)

    res = s.get("https://google.com")
    res.status_code == 200, "headers should be valid and accepted"
