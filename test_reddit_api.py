#!/usr/bin/env python3
"""
Test script to debug Reddit API scraping issues
"""

import requests
import json
from datetime import datetime, timezone, timedelta
import sys
import os

# Add the data_scraping directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'data_scraping'))

from reddit_scraper import RedditScrapeService

def test_reddit_search_api():
    """Test Reddit's search API directly"""
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    # Test different search approaches
    subreddit_name = "CryptoCurrency"
    
    # 1. Test basic subreddit search (no date filter)
    print("=== Testing basic subreddit search ===")
    search_url = f"https://www.reddit.com/r/{subreddit_name}/search.json"
    params = {
        'q': '*',
        'sort': 'new',
        't': 'hour',  # Last hour
        'limit': 10
    }
    
    try:
        response = requests.get(search_url, headers=headers, params=params, timeout=10)
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"Response keys: {data.keys()}")
            
            if 'data' in data and 'children' in data['data']:
                posts = data['data']['children']
                print(f"Found {len(posts)} posts")
                
                if posts:
                    for i, post_data in enumerate(posts[:3]):
                        post = post_data['data']
                        print(f"\nPost {i+1}:")
                        print(f"  Title: {post.get('title', 'N/A')}")
                        print(f"  Created UTC: {post.get('created_utc', 'N/A')}")
                        print(f"  Created Date: {datetime.fromtimestamp(post.get('created_utc', 0), tz=timezone.utc)}")
                        print(f"  Subreddit: {post.get('subreddit', 'N/A')}")
                        print(f"  Score: {post.get('score', 'N/A')}")
                else:
                    print("No posts found in response")
            else:
                print("Unexpected response structure")
                print(f"Data: {data}")
        else:
            print(f"Error: {response.status_code}")
            print(f"Response: {response.text[:500]}")
            
    except Exception as e:
        print(f"Exception: {e}")
    
    # 2. Test timestamp-based search
    print("\n=== Testing timestamp-based search ===")
    now = datetime.now(timezone.utc)
    start_time = now - timedelta(hours=1)  # Last hour
    start_timestamp = int(start_time.timestamp())
    end_timestamp = int(now.timestamp())
    
    print(f"Searching for posts between {start_time} and {now}")
    print(f"Timestamps: {start_timestamp} to {end_timestamp}")
    
    params_timestamp = {
        'q': f'timestamp:{start_timestamp}..{end_timestamp}',
        'sort': 'new',
        't': 'all',
        'limit': 10
    }
    
    try:
        response = requests.get(search_url, headers=headers, params=params_timestamp, timeout=10)
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"Response keys: {data.keys()}")
            
            if 'data' in data and 'children' in data['data']:
                posts = data['data']['children']
                print(f"Found {len(posts)} posts with timestamp search")
                
                if posts:
                    for i, post_data in enumerate(posts[:3]):
                        post = post_data['data']
                        post_timestamp = post.get('created_utc', 0)
                        print(f"\nPost {i+1}:")
                        print(f"  Title: {post.get('title', 'N/A')}")
                        print(f"  Created UTC: {post_timestamp}")
                        print(f"  Created Date: {datetime.fromtimestamp(post_timestamp, tz=timezone.utc)}")
                        print(f"  In range: {start_timestamp <= post_timestamp <= end_timestamp}")
                else:
                    print("No posts found with timestamp search")
            else:
                print("Unexpected response structure with timestamp search")
                print(f"Data: {data}")
        else:
            print(f"Error with timestamp search: {response.status_code}")
            print(f"Response: {response.text[:500]}")
            
    except Exception as e:
        print(f"Exception with timestamp search: {e}")

def test_scraper_service():
    """Test the RedditScrapeService directly"""
    print("\n=== Testing RedditScrapeService ===")
    
    scraper = RedditScrapeService()
    
    # Test the regular scrape_subreddit method
    print("Testing regular subreddit scraping...")
    try:
        posts = scraper.scrape_subreddit("https://www.reddit.com/r/CryptoCurrency/", limit=5)
        print(f"Regular scraping found {len(posts)} posts")
        
        if posts:
            for i, post in enumerate(posts[:2]):
                print(f"\nPost {i+1}:")
                print(f"  ID: {post.get('id', 'N/A')}")
                print(f"  Title snippet: {post.get('text_clean', 'N/A')[:100]}...")
                print(f"  Timestamp: {post.get('post_created_timestamp', 'N/A')}")
    except Exception as e:
        print(f"Error with regular scraping: {e}")
    
    # Test the scrape_posts_by_date_range method
    print("\nTesting date range scraping...")
    try:
        now = datetime.now(timezone.utc)
        start_time = now - timedelta(hours=24)  # Last 24 hours
        start_timestamp = start_time.timestamp()
        end_timestamp = now.timestamp()
        
        print(f"Date range: {start_time} to {now}")
        print(f"Timestamps: {start_timestamp} to {end_timestamp}")
        
        posts = scraper.scrape_posts_by_date_range(
            subreddit_url="https://www.reddit.com/r/CryptoCurrency/",
            start_date=str(start_timestamp),
            end_date=str(end_timestamp),
            limit=10
        )
        
        print(f"Date range scraping found {len(posts)} posts")
        
        if posts:
            for i, post in enumerate(posts[:2]):
                print(f"\nPost {i+1}:")
                print(f"  ID: {post.get('id', 'N/A')}")
                print(f"  Title snippet: {post.get('text_clean', 'N/A')[:100]}...")
                print(f"  Timestamp: {post.get('post_created_timestamp', 'N/A')}")
        else:
            print("No posts found with date range scraping")
            
    except Exception as e:
        print(f"Error with date range scraping: {e}")

if __name__ == "__main__":
    test_reddit_search_api()
    test_scraper_service()