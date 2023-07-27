#!/bin/bash
if [[ ! $(pgrep -f ica) ]]; then
	cd ~/mortgage-scraper && source venv/bin/activate && python -m mortgage_scraper -t ica -s csv --delay 0.4 --randomize &
else
	echo "ica scraper already running, not starting new process"
fi

if [[ ! $(pgrep -f sbab ) ]]; then
	cd ~/mortgage-scraper && source venv/bin/activate && python -m mortgage_scraper -t sbab -s csv --randomize &
else
	echo "sbab scraper already running, not starting new process"
fi

if [[ ! $(pgrep -f hypoteket ) ]]; then
	cd ~/mortgage-scraper && source venv/bin/activate && python -m mortgage_scraper -t hypoteket -s csv --randomize &
else
	echo "hypoteket scraper already running, not starting new process"
fi

if [[ ! $(pgrep -f skandia ) ]]; then
	cd ~/mortgage-scraper && source venv/bin/activate && python -m mortgage_scraper -t skandia -s csv --randomize --rotate-user-agent &
else
	echo "skandia scraper already running, not starting new process"
fi
