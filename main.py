import sys
import os
import logging
from datetime import datetime
import time

# Add paths for imports
sys.path.append(os.path.join(os.path.dirname(__file__), 'cronjob'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'kafka'))

from reddit_scheduler import RedditScraperScheduler
from reddit_producer import RedditKafkaProducer

def main():
    """Main function to run Reddit scraper with Kafka integration"""
    print("Starting Reddit scraper with Kafka integration...")
    
    # Initialize components
    scheduler = RedditScraperScheduler()
    producer = RedditKafkaProducer()
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    def scrape_and_send_5m():
        """Scrape Reddit posts from last 5 minutes and send to Kafka"""
        logger.info("Running 5-minute scraping and Kafka send...")
        posts = scheduler.scrape_reddit_posts_and_return(5, "5-minute")
        if posts:
            logger.info(f"Sending {len(posts)} posts to Kafka...")
            successful = producer.send_batch(posts, 'post')
            logger.info(f"Successfully sent {successful}/{len(posts)} posts to Kafka")
        else:
            logger.info("No posts found to send to Kafka")

        """Scrape Reddit posts from last 4 hours and send to Kafka"""
        logger.info("Running 4-hour scraping and Kafka send...")
        posts = scheduler.scrape_reddit_posts_and_return(240, "4-hour")
        if posts:
            logger.info(f"Sending {len(posts)} posts to Kafka...")
            successful = producer.send_batch(posts, 'post')
            logger.info(f"Successfully sent {successful}/{len(posts)} posts to Kafka")
        else:
            logger.info("No posts found to send to Kafka")
    
    # Update scheduler methods to include Kafka sending
    original_scrape_5m = scheduler.scrape_5m
    
    # Override methods to include Kafka integration
    def scrape_5m_with_kafka():
        original_scrape_5m()
        scrape_and_send_5m()
    
    
    # Replace scheduler methods
    scheduler.scrape_5m = scrape_5m_with_kafka
    
    # Run initial scraping and send to Kafka
    logger.info("Running initial Reddit scraping and sending to Kafka...")
    scrape_and_send_5m()
    
    # Start the scheduler
    logger.info("Starting scheduler with Kafka integration...")
    try:
        scheduler.start()
    except KeyboardInterrupt:
        logger.info("Stopping scheduler...")
        producer.close()
        scheduler.stop()
    except Exception as e:
        logger.error(f"Error: {e}")
        producer.close()
        scheduler.stop()


if __name__ == "__main__":
    main()
