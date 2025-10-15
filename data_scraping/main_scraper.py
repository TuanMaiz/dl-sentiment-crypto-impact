import json
import os
from datetime import datetime
from typing import List
from reddit_scraper import RedditScrapeService
from binance_scraper import BinanceScrapeService


class DataScrapingOrchestrator:
    def __init__(self, sites_file: str = "sites_to_scrape.txt"):
        self.sites_file = sites_file
        self.reddit_service = RedditScrapeService()
        self.binance_service = BinanceScrapeService()
        self.output_dir = "./output"
        
        # Create output directory if it doesn't exist
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
    
    def load_sites_to_scrape(self) -> list[str]:
        """Load sites from the sites_to_scrape.txt file"""
        sites = []
        try:
            with open(self.sites_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and line.startswith('https://www.reddit.com'):
                        sites.append(line)
        except FileNotFoundError:
            print(f"Error: {self.sites_file} not found")
        except Exception as e:
            print(f"Error reading sites file: {str(e)}")
        
        return sites
    
    def scrape_all_sites(self, limit_per_site: int = 25):
        """Scrape all Reddit sites from the sites file"""
        sites = self.load_sites_to_scrape()
        
        if not sites:
            print("No Reddit sites found to scrape")
            return
        
        all_posts = []
        
        for site in sites:
            print(f"Scraping: {site}")
            try:
                posts = self.reddit_service.scrape_subreddit(site, limit=limit_per_site)
                all_posts.extend(posts)
                print(f"  Scraped {len(posts)} posts")
            except Exception as e:
                print(f"  Error scraping {site}: {str(e)}")
        
        # Save all posts to a JSON file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(self.output_dir, f"reddit_posts_{timestamp}.json")
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(all_posts, f, indent=2, ensure_ascii=False)
        
        print(f"\nSaved {len(all_posts)} total posts to {output_file}")
        return output_file
    
    def scrape_single_site(self, url: str, limit: int = 25, output_file: str = None):
        """Scrape a single Reddit site"""
        print(f"Scraping: {url}")
        posts = self.reddit_service.scrape_subreddit(url, limit=limit)
        
        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = os.path.join(self.output_dir, f"reddit_posts_{timestamp}.json")
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(posts, f, indent=2, ensure_ascii=False)
        
        print(f"Saved {len(posts)} posts to {output_file}")
        return output_file
    
    def scrape_site_by_date_range(self, url: str, start_date: str, end_date: str, limit: int = 100, output_file: str = None):
        """Scrape a single Reddit site within a specific date range"""
        print(f"Scraping posts from {url} between {start_date} and {end_date}")
        posts = self.reddit_service.scrape_posts_by_date_range(url, start_date, end_date, limit=limit)
        
        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = os.path.join(self.output_dir, f"reddit_posts_{start_date.replace('/', '')}_to_{end_date.replace('/', '')}_{timestamp}.json")
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(posts, f, indent=2, ensure_ascii=False)
        
        print(f"Saved {len(posts)} posts to {output_file}")
        return output_file
    
    def scrape_crypto_data(self, symbols: List[str] = None, interval: str = '1h', limit: int = 100):
        """Scrape cryptocurrency OHLCV data"""
        if symbols is None:
            symbols = ['BTCUSDT', 'ETHUSDT']
        
        print(f"Scraping crypto data for {symbols} with {interval} interval")
        crypto_data = self.binance_service.get_multiple_crypto_data(symbols, interval, limit)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(self.output_dir, f"crypto_ohlcv_{timestamp}.json")
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(crypto_data, f, indent=2, ensure_ascii=False)
        
        print(f"Saved {len(crypto_data)} data points to {output_file}")
        return output_file
    
    def scrape_crypto_by_date_range(self, symbols: List[str] = None, start_date: str = None, end_date: str = None, interval: str = '1h'):
        """Scrape cryptocurrency data within a date range"""
        if symbols is None:
            symbols = ['BTCUSDT', 'ETHUSDT']
        
        if start_date is None:
            start_date = "01/10/2025"
        if end_date is None:
            end_date = datetime.now().strftime("%d/%m/%Y")
        
        all_data = []
        
        for symbol in symbols:
            print(f"Scraping {symbol} data from {start_date} to {end_date}")
            crypto_data = self.binance_service.get_crypto_data_by_date_range(symbol, start_date, end_date, interval)
            all_data.extend(crypto_data)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(self.output_dir, f"crypto_ohlcv_{start_date.replace('/', '')}_to_{end_date.replace('/', '')}_{timestamp}.json")
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(all_data, f, indent=2, ensure_ascii=False)
        
        print(f"Saved {len(all_data)} data points to {output_file}")
        return output_file
    
    def get_current_crypto_prices(self, symbols: List[str] = None):
        """Get current prices for cryptocurrencies"""
        if symbols is None:
            symbols = ['BTCUSDT', 'ETHUSDT']
        
        current_prices = []
        
        for symbol in symbols:
            price_data = self.binance_service.get_current_price(symbol)
            if price_data:
                current_prices.append(price_data)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(self.output_dir, f"crypto_current_prices_{timestamp}.json")
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(current_prices, f, indent=2, ensure_ascii=False)
        
        print(f"Saved current prices for {len(current_prices)} symbols to {output_file}")
        return output_file


if __name__ == "__main__":
    orchestrator = DataScrapingOrchestrator()
    
    # Example: Scrape cryptocurrency OHLCV data for BTC and ETH
    orchestrator.scrape_crypto_data(
        symbols=['BTCUSDT', 'ETHUSDT'],
        interval='15m',
        limit=100
    )
    
    # Example: Scrape crypto data by date range
    # orchestrator.scrape_crypto_by_date_range(
    #     symbols=['BTCUSDT', 'ETHUSDT'],
    #     start_date="01/10/2025",
    #     end_date="15/10/2025",
    #     interval='1h'
    # )
    
    # Example: Get current prices
    # orchestrator.get_current_crypto_prices(['BTCUSDT', 'ETHUSDT'])
    
    # Or scrape Reddit posts from 01/10/2025 to today
    # orchestrator.scrape_site_by_date_range(
    #     url="https://www.reddit.com/r/CryptoCurrency/",
    #     start_date="01/10/2025",
    #     end_date="15/10/2025",
    #     limit=50
    # )
    
    # Or scrape all sites from the file
    # orchestrator.scrape_all_sites(limit_per_site=10)
    
    # Or scrape a single site
    # orchestrator.scrape_single_site("https://www.reddit.com/r/CryptoCurrency/", limit=20)