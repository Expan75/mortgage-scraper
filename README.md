# Mortgage Scraper

### Getting started

```bash
# setup and install dependencies in isolated virtual environmnet
python3 -m venv venv && source venv/bin/activate && pip install -r requirements.txt

# scrape ica banken and export as .csv
(venv) python main.py -t ica -s csv
(venv) python main.py --target ica --sink csv

# specifying multiple targets
(venv) python main.py -t ica -t skandia -s csv
(venv) python main.py --target ica --target skandia --store csv

# List all CLI options
(venv) python main.py --help

# Run with debug logging
(venv) python main.py -t ica -s csv -d
(venv) python main.py --target ica -sink csv --debug
```

### Advanced Options

```bash
# Using proxy and sending a single request 
(venv) python main.py -t ica -s csv \
    --proxy user:pass@https://someproxy.idk \
    --urls-limit 1 \

# Full scan but random order and rotatating user agent
(venv) python main.py -t ica -s csv \
    --randomise-url-order \
    --seed 42 \ 
    --rotate-user-agent

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

# run but avoid hitting a provider if you've been ip-banned/blocked
(venv) pytest -k "not skandia"

# NOTE: for E2E tests, some assumptions are made
# - E2E tests require exec. rights on main.
# - E2E test with proxy requires PROXY to be set to a valid forward proxy
```

### Formatting and linting

This project uses black8 and flake8 for linting. To run the same linter as applied in CI/CD, use:

```bash
(venv) python -m flake8 /mortgage_scraper

# alt. run black auto formatter. This does not neccesarrily catch everything!
(venv) python -m black ./
```

