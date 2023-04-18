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
```

### Project Structure

The project primarily consists of the scraper, which is really individual scrapers adapted for each API target. This are combined in a CLI workflow that enables one to compare different pricing models across the board. There are also EDA notebooks that enables one to generate pricing surfaces based off of the mined data.

In short:

```bash
/notebooks      # contains pricing surface generation and EDA examples
/src            # source code for scraper
main.py         # cli entry point
```