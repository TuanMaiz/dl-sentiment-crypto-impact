#!/usr/bin/env python3
"""
Crypto Scraper Scheduler using APScheduler
Runs crypto data scraping tasks at different intervals (5m, 15m, 1h, 4h)
"""

import sys
import os
import logging
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

# Add the data_scraping directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'data_scraping'))

from main_scraper import DataScrapingOrchestrator


class CryptoScraperScheduler:
    def __init__(self):
        self.scheduler = BlockingScheduler()
        self.orchestrator = DataScrapingOrchestrator()
        self.setup_logging()
        
    def setup_logging(self):
        """Setup logging configuration"""
        # Create logs directory if it doesn't exist
        log_dir = os.path.join(os.path.dirname(__file__), 'logs')
        os.makedirs(log_dir, exist_ok=True)
        
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(os.path.join(log_dir, 'crypto_scheduler.log')),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def scrape_5m(self):
        """Scrape crypto data with 5-minute interval (latest data only)"""
        self.logger.info("Starting 5-minute crypto data scraping")
        try:
            self.orchestrator.scrape_crypto_data(
                symbols=['BTCUSDT', 'ETHUSDT'],
                interval='5m',
                limit=1  # Latest data point only
            )
            self.logger.info("5-minute crypto scraping completed successfully")
        except Exception as e:
            self.logger.error(f"5-minute crypto scraping failed: {str(e)}")
            
    def scrape_15m(self):
        """Scrape crypto data with 15-minute interval (latest data only)"""
        self.logger.info("Starting 15-minute crypto data scraping")
        try:
            self.orchestrator.scrape_crypto_data(
                symbols=['BTCUSDT', 'ETHUSDT'],
                interval='15m',
                limit=1  # Latest data point only
            )
            self.logger.info("15-minute crypto scraping completed successfully")
        except Exception as e:
            self.logger.error(f"15-minute crypto scraping failed: {str(e)}")
            
    def scrape_1h(self):
        """Scrape crypto data with 1-hour interval (latest data only)"""
        self.logger.info("Starting 1-hour crypto data scraping")
        try:
            self.orchestrator.scrape_crypto_data(
                symbols=['BTCUSDT', 'ETHUSDT'],
                interval='1h',
                limit=1  # Latest data point only
            )
            self.logger.info("1-hour crypto scraping completed successfully")
        except Exception as e:
            self.logger.error(f"1-hour crypto scraping failed: {str(e)}")
            
    def scrape_4h(self):
        """Scrape crypto data with 4-hour interval (latest data only)"""
        self.logger.info("Starting 4-hour crypto data scraping")
        try:
            self.orchestrator.scrape_crypto_data(
                symbols=['BTCUSDT', 'ETHUSDT'],
                interval='4h',
                limit=1  # Latest data point only
            )
            self.logger.info("4-hour crypto scraping completed successfully")
        except Exception as e:
            self.logger.error(f"4-hour crypto scraping failed: {str(e)}")
    
    def job_listener(self, event):
        """Listener for job execution events"""
        if event.exception:
            self.logger.error(f"Job {event.job_id} failed: {event.exception}")
        else:
            self.logger.info(f"Job {event.job_id} executed successfully")
    
    def setup_jobs(self):
        """Setup all scheduled jobs"""
        # Add event listener
        self.scheduler.add_listener(self.job_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
        
        # 5-minute interval job
        self.scheduler.add_job(
            self.scrape_5m,
            IntervalTrigger(minutes=5),
            id='crypto_5m',
            name='Crypto Data Scraping - 5 minutes',
            max_instances=1,
            coalesce=True
        )
        
        # 15-minute interval job
        self.scheduler.add_job(
            self.scrape_15m,
            IntervalTrigger(minutes=15),
            id='crypto_15m',
            name='Crypto Data Scraping - 15 minutes',
            max_instances=1,
            coalesce=True
        )
        
        # 1-hour interval job
        self.scheduler.add_job(
            self.scrape_1h,
            IntervalTrigger(hours=1),
            id='crypto_1h',
            name='Crypto Data Scraping - 1 hour',
            max_instances=1,
            coalesce=True
        )
        
        # 4-hour interval job
        self.scheduler.add_job(
            self.scrape_4h,
            IntervalTrigger(hours=4),
            id='crypto_4h',
            name='Crypto Data Scraping - 4 hours',
            max_instances=1,
            coalesce=True
        )
        
        self.logger.info("All crypto scraper jobs have been scheduled")
        self.logger.info("Jobs scheduled:")
        self.logger.info("- 5-minute interval: crypto_5m")
        self.logger.info("- 15-minute interval: crypto_15m")
        self.logger.info("- 1-hour interval: crypto_1h")
        self.logger.info("- 4-hour interval: crypto_4h")
    
    def start(self):
        """Start the scheduler"""
        self.logger.info("Starting Crypto Scraper Scheduler...")
        self.logger.info("Press Ctrl+C to stop the scheduler")
        
        try:
            self.scheduler.start()
        except KeyboardInterrupt:
            self.logger.info("Scheduler stopped by user")
            self.scheduler.shutdown()
        except Exception as e:
            self.logger.error(f"Scheduler error: {str(e)}")
            self.scheduler.shutdown()
    
    def stop(self):
        """Stop the scheduler"""
        self.scheduler.shutdown()
        self.logger.info("Scheduler stopped")


def main():
    """Main function to run the scheduler"""
    scheduler = CryptoScraperScheduler()
    scheduler.setup_jobs()
    
    # Run initial scrape for all intervals
    print("Running initial crypto data scraping...")
    scheduler.scrape_5m()
    scheduler.scrape_15m()
    scheduler.scrape_1h()
    scheduler.scrape_4h()
    
    # Start the scheduler
    scheduler.start()


if __name__ == "__main__":
    main()