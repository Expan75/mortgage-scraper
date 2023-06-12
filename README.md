# Mortgage Scraper

### Getting started

```bash
# setup and install dependencies in isolated virtual environmnet
python3 -m venv venv && source venv/bin/activate && pip install -r requirements.txt

# scrape ica banken and export as .csv
(venv) python main.py -t ica -s csv
(venv) python main.py --target ica --store csv

# specifying multiple targets
(venv) python main.py -t ica,skandia,hypoteket -t csv
(venv) python main.py --target ica,skandia,hypoteket --store csv

# List all CLI options
(venv) python main.py --help

# Using proxy and sending a single request to each target
(venv) python main.py -t ica,skandia,hypoteket,sbab -s csv \
    --proxy user:pass@https://someproxy.idk \
    --limit 1
```

### Project Structure

The project primarily consists of the scraper, which is really individual scrapers adapted for each API target. This are combined in a CLI workflow that enables one to compare different pricing models across the board. There are also EDA notebooks that enables one to generate pricing surfaces based off of the mined data.

In short:

```bash
/notebooks      # contains pricing surface generation and EDA examples
/src            # source code for scraper
/tests          # test directory
main.py         # cli entry point
```

### Tests

```bash
# run all tests
(venv) pytest

# run only e2e
(venv) pytest tests/test_e2e.py

# NOTE: for E2E tests, some assumptions are made
# - E2E tests require exec. rights on main.
# - E2E test with proxy requires PROXY to be set to a valid forward proxy
```
