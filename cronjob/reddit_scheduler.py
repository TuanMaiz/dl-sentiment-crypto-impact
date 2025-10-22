#!/usr/bin/env python3
"""
Reddit Scraper Scheduler using APScheduler
Runs Reddit scraping tasks at different intervals (5m, 15m, 1h, 4h)
Each interval scrapes posts from the corresponding time period
"""

import sys
import os
import logging
from datetime import datetime, timezone, timedelta
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
import json

# Add the data_scraping directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'data_scraping'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))

from reddit_scraper import RedditScrapeService
from lang_detect import LangUtils

class RedditScraperScheduler:
    def __init__(self):
        self.scheduler = BlockingScheduler()
        self.scraper = RedditScrapeService()
        self.setup_logging()
        
        # Configure subreddits to scrape from sites_to_scrape.txt
        self.subreddits = self.load_subreddits_from_file()
    
    def load_subreddits_from_file(self):
        """Load subreddits from sites_to_scrape.txt file"""
        try:
            file_path = os.path.join(os.path.dirname(__file__), 'sites_to_scrape.txt')
            with open(file_path, 'r', encoding='utf-8') as f:
                subreddits = [line.strip() for line in f.readlines() if line.strip()]
            
            if not subreddits:
                self.logger.warning("No subreddits found in sites_to_scrape.txt, using default list")
                return [
                    "https://www.reddit.com/r/CryptoCurrency/",
                    "https://www.reddit.com/r/Bitcoin/",
                    "https://www.reddit.com/r/Ethereum/",
                    "https://www.reddit.com/r/CryptoMarkets/",
                ]
            
            self.logger.info(f"Loaded {len(subreddits)} subreddits from sites_to_scrape.txt")
            return subreddits
        except FileNotFoundError:
            self.logger.warning("sites_to_scrape.txt not found, using default list")
            return [
                "https://www.reddit.com/r/CryptoCurrency/",
                "https://www.reddit.com/r/Bitcoin/",
                "https://www.reddit.com/r/Ethereum/",
                "https://www.reddit.com/r/CryptoMarkets/",
            ]
        except Exception as e:
            self.logger.error(f"Error reading sites_to_scrape.txt: {e}, using default list")
            return [
                "https://www.reddit.com/r/CryptoCurrency/",
                "https://www.reddit.com/r/Bitcoin/",
                "https://www.reddit.com/r/Ethereum/",
                "https://www.reddit.com/r/CryptoMarkets/",
            ]
        
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
                logging.FileHandler(os.path.join(log_dir, 'reddit_scheduler.log')),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def scrape_reddit_posts(self, interval_minutes: int, job_name: str):
        """
        Scrape Reddit posts from the specified time interval
        
        Args:
            interval_minutes: Number of minutes to look back
            job_name: Name of the job for logging
        """
        self.logger.info(f"Starting {job_name} Reddit scraping (last {interval_minutes} minutes)")
        
        # Include comments for longer time intervals to get more data
        include_comments = interval_minutes >= 60  # Only include comments for 1h+ intervals
        
        try:
            # Calculate time range
            now = datetime.now(timezone.utc)
            start_time = now - timedelta(minutes=interval_minutes)
            
            # Convert to Unix timestamps
            start_timestamp = start_time.timestamp()
            end_timestamp = now.timestamp()
            
            all_posts = []
            total_scraped = 0
            
            for subreddit_url in self.subreddits:
                try:
                    # Try date range scraping first
                    posts = self.scraper.scrape_posts_by_date_range(
                        subreddit_url=subreddit_url,
                        start_date=str(start_timestamp),
                        end_date=str(end_timestamp),
                        limit=50,  # Adjust limit as needed
                        include_comments=include_comments
                    )
                    
                    subreddit_posts = len(posts)
                    total_scraped += subreddit_posts
                    self.logger.info(f"Found {subreddit_posts} posts from {subreddit_url}")
                    all_posts.extend(posts)
                    
                except Exception as e:
                    self.logger.error(f"Error scraping {subreddit_url}: {e}")
            
            # Log summary
            total_comments = sum(len(post.get('comments', [])) for post in all_posts)
            self.logger.info(f"Total scraped from all subreddits: {total_scraped} posts with {total_comments} comments")
            
            # Save results if any posts found
            if all_posts:
                self.save_posts(all_posts, interval_minutes, now)
                if include_comments:
                    self.logger.info(f"{job_name} Reddit scraping completed successfully - {len(all_posts)} posts with {total_comments} comments")
                else:
                    self.logger.info(f"{job_name} Reddit scraping completed successfully - {len(all_posts)} posts (no comments)")
            else:
                # Try fallback: get recent posts and see if any are within the time window
                self.logger.info(f"No posts found in last {interval_minutes} minutes, checking recent posts...")
                
                for subreddit_url in self.subreddits:
                    try:
                        recent_posts = self.scraper.scrape_subreddit(subreddit_url, limit=20)
                        
                        for post in recent_posts:
                            try:
                                post_time = datetime.fromisoformat(post['post_created_timestamp'].replace('Z', '+00:00'))
                                if start_time <= post_time <= now:
                                    all_posts.append(post)
                            except Exception as e:
                                continue
                        
                        if len([p for p in recent_posts if start_time <= datetime.fromisoformat(p['post_created_timestamp'].replace('Z', '+00:00')) <= now]) > 0:
                            self.logger.info(f"Found posts in time window from {subreddit_url}")
                    
                    except Exception as e:
                        self.logger.error(f"Error checking recent posts from {subreddit_url}: {e}")
                
                if all_posts:
                    self.save_posts(all_posts, interval_minutes, now)
                    self.logger.info(f"{job_name} Reddit scraping completed with fallback - {len(all_posts)} total posts")
                else:
                    self.logger.info(f"No new posts found in last {interval_minutes} minutes (tried both methods)")
                
        except Exception as e:
            self.logger.error(f"{job_name} Reddit scraping failed: {str(e)}")
    
    def scrape_reddit_posts_and_return(self, interval_minutes: int, job_name: str) -> list:
        """
        Scrape Reddit posts from the specified time interval and return them
        
        Args:
            interval_minutes: Number of minutes to look back
            job_name: Name of the job for logging
            
        Returns:
            List of scraped Reddit posts
        """
        self.logger.info(f"Starting {job_name} Reddit scraping (last {interval_minutes} minutes) - returning data")
        
        # Include comments for longer time intervals to get more data
        include_comments = interval_minutes >= 60  # Only include comments for 1h+ intervals
        
        try:
            # Calculate time range
            now = datetime.now(timezone.utc)
            start_time = now - timedelta(minutes=interval_minutes)
            
            # Convert to Unix timestamps
            start_timestamp = start_time.timestamp()
            end_timestamp = now.timestamp()
            
            all_posts = []
            total_scraped = 0
            
            for subreddit_url in self.subreddits:
                try:
                    # Try date range scraping first
                    posts = self.scraper.scrape_posts_by_date_range(
                        subreddit_url=subreddit_url,
                        start_date=str(start_timestamp),
                        end_date=str(end_timestamp),
                        limit=50,  # Adjust limit as needed
                        include_comments=include_comments
                    )
                    
                    subreddit_posts = len(posts)
                    total_scraped += subreddit_posts
                    self.logger.info(f"Found {subreddit_posts} posts from {subreddit_url}")
                    all_posts.extend(posts)
                    
                except Exception as e:
                    self.logger.error(f"Error scraping {subreddit_url}: {e}")
            
            # Log summary
            total_comments = sum(len(post.get('comments', [])) for post in all_posts)
            self.logger.info(f"Total scraped from all subreddits: {total_scraped} posts with {total_comments} comments")
            
            # Return posts if any found
            if all_posts:
                if include_comments:
                    self.logger.info(f"{job_name} Reddit scraping completed successfully - {len(all_posts)} posts with {total_comments} comments")
                else:
                    self.logger.info(f"{job_name} Reddit scraping completed successfully - {len(all_posts)} posts (no comments)")
                return all_posts
            else:
                # Try fallback: get recent posts and see if any are within the time window
                self.logger.info(f"No posts found in last {interval_minutes} minutes, checking recent posts...")
                
                fallback_posts = []
                for subreddit_url in self.subreddits:
                    try:
                        recent_posts = self.scraper.scrape_subreddit(subreddit_url, limit=20)
                        
                        for post in recent_posts:
                            try:
                                post_time = datetime.fromisoformat(post['post_created_timestamp'].replace('Z', '+00:00'))
                                if start_time <= post_time <= now:
                                    fallback_posts.append(post)
                            except Exception as e:
                                continue
                        
                        if len([p for p in recent_posts if start_time <= datetime.fromisoformat(p['post_created_timestamp'].replace('Z', '+00:00')) <= now]) > 0:
                            self.logger.info(f"Found posts in time window from {subreddit_url}")
                    
                    except Exception as e:
                        self.logger.error(f"Error checking recent posts from {subreddit_url}: {e}")
                
                if fallback_posts:
                    self.logger.info(f"{job_name} Reddit scraping completed with fallback - {len(fallback_posts)} total posts")
                    return fallback_posts
                else:
                    self.logger.info(f"No new posts found in last {interval_minutes} minutes (tried both methods)")
                    return []
                
        except Exception as e:
            self.logger.error(f"{job_name} Reddit scraping failed: {str(e)}")
            return []
    
    def scrape_5m(self):
        """Scrape Reddit posts from the last 5 minutes"""
        self.scrape_reddit_posts(5, "5-minute")        
    
    def save_posts(self, posts: list, interval_minutes: int, timestamp: datetime):
        """Save posts to JSON file with timestamp"""
        # Create output directory if it doesn't exist
        output_dir = os.path.join(os.path.dirname(__file__), '..', 'output_reddit')
        os.makedirs(output_dir, exist_ok=True)
        
        # Save with timestamp and interval in filename
        timestamp_str = timestamp.strftime("%Y%m%d_%H%M")
        filename = f"{output_dir}/reddit_posts_{interval_minutes}m_{timestamp_str}.json"
        
        # Add metadata to the file
        output_data = {
            "metadata": {
                "scraped_at": timestamp.isoformat(),
                "interval_minutes": interval_minutes,
                "total_posts": len(posts),
                "subreddits_scraped": self.subreddits
            },
            "posts": posts
        }
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"Saved {len(posts)} posts to {filename}")
    
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
            id='reddit_5m',
            name='Reddit Scraping - 5 minutes',
            max_instances=1,
            coalesce=True
        )
                
        
        self.logger.info("All Reddit scraper jobs have been scheduled")
        self.logger.info("Jobs scheduled:")
        self.logger.info("- 5-minute interval: reddit_5m (scrapes last 5 minutes)")

    
    def start(self):
        """Start the scheduler"""
        self.logger.info("Starting Reddit Scraper Scheduler...")
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
    scheduler = RedditScraperScheduler()
    scheduler.setup_jobs()
    
    # Run initial scrape for all intervals
    print("Running initial Reddit scraping...")
    # scheduler.scrape_5m()
    # scheduler.scrape_15m()
    scheduler.scrape_1h()
    scheduler.scrape_4h()
    
    # Start the scheduler
    scheduler.start()


if __name__ == "__main__":
    main()