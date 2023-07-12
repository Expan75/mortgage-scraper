# Mortgage Scraper

### Getting started

```bash
# setup and install dependencies in isolated virtual environmnet
python3 -m venv venv && source venv/bin/activate && pip install -r requirements.txt

# scrape ica banken and export as .csv
(venv) python -m mortgage_scraper -t ica -s csv
(venv) python -m mortgage_scraper --target ica --sink csv

# specifying multiple targets
(venv) python -m mortgage_scraper -t ica hypoteket -s csv
(venv) python -m mortgage_scraper --target ica hypoteket -s csv

# List all CLI options
(venv) python -m mortgage_scraper --help

# Run with debug logging
(venv) python -m mortgage_scraper  -t ica -s csv -d
(venv) python -m mortgage_scraper --target ica --sink csv --debug
```

### Advanced Options

```bash
# Using proxy and sending a single request
(venv) python -m mortgage_scraper -t ica -s csv \
    --proxy user:pass@https://someproxy.idk \
    --urls-limit 1 \

# Full scan but random order and rotatating user agent
(venv) python -m mortgage_scraper -t ica -s csv \
    --randomize \
    --seed 42 \
    --rotate-user-agent
```

### Project Structure

```bash
/mortgage_scraper           # source code for scraper
/tests                      # test directory
```

### Tests

```bash
# run all tests
(venv) pytest -k "not skandia" # skandia has shut off their endpoint at time of writing.

# run only e2e
(venv) pytest tests/test_e2e.py
```

### Formatting and linting

This project uses black8 and flake8 for linting. To run the same linter as applied in CI/CD, use:

```bash
# run formatter to avoid getting rejected by lint check in CI/CD.
(venv) python -m black ./mortgage_scraper /tests

# if something slips through formatting still and is rejected, you can manually run
# linting on your code via:
(venv) python -m flake8 /mortgage_scraper
```
