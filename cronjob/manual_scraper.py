#!/usr/bin/env python3
"""
Standalone Crypto Scraper Script
Can be used for manual execution or testing
"""

import sys
import os
from datetime import datetime

# Add the data_scraping directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'data_scraping'))

from data_scraping.main_scraper import DataScrapingOrchestrator


def run_crypto_scraper(interval='1h', limit=100):
    """Run crypto scraper with specified parameters"""
    print(f"Running crypto scraper - Interval: {interval}, Limit: {limit}")
    print("-" * 50)
    
    orchestrator = DataScrapingOrchestrator()
    
    try:
        # Get current crypto prices
        print("Fetching current prices...")
        orchestrator.get_current_crypto_prices(['BTCUSDT', 'ETHUSDT'])
        
        # Scrape OHLCV data
        print(f"Scraping OHLCV data with {interval} interval...")
        orchestrator.scrape_crypto_data(
            symbols=['BTCUSDT', 'ETHUSDT'],
            interval=interval,
            limit=limit
        )
        
        print("-" * 50)
        print("Crypto scraping completed successfully!")
        
    except Exception as e:
        print(f"Error during crypto scraping: {str(e)}")
        return False
    
    return True


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Manual Crypto Scraper')
    parser.add_argument('--interval', default='1h', 
                       choices=['5m', '15m', '1h', '4h'],
                       help='Data interval (default: 1h)')
    parser.add_argument('--limit', type=int, default=100,
                       help='Number of data points to fetch (default: 100)')
    
    args = parser.parse_args()
    
    success = run_crypto_scraper(args.interval, args.limit)
    sys.exit(0 if success else 1)