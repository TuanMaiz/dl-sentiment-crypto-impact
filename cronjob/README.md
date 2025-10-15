# Crypto Scraper Scheduler

This directory contains the Python-based scheduler for automated cryptocurrency data scraping using APScheduler.

## Files

- `crypto_scheduler.py` - Main scheduler with 5m, 15m, 1h, 4h intervals
- `manual_scraper.py` - Standalone script for manual execution
- `requirements.txt` - Python dependencies

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Start the scheduler:
```bash
python crypto_scheduler.py
```

## Manual Usage

Run scraper manually with custom parameters:
```bash
# Default 1-hour interval
python manual_scraper.py

# Custom interval and limit
python manual_scraper.py --interval 15m --limit 50
```

## Scheduled Intervals

- **5 minutes**: Fires scrape method every 5 minutes (latest data point)
- **15 minutes**: Fires scrape method every 15 minutes (latest data point)  
- **1 hour**: Fires scrape method every 1 hour (latest data point)
- **4 hours**: Fires scrape method every 4 hours (latest data point)

## Logging

All activities are logged to `../logs/crypto_scheduler.log`

## Stop Scheduler

Press `Ctrl+C` to stop the running scheduler.