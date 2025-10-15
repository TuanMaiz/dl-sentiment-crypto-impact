import json
import os
from datetime import datetime
from reddit_scraper import RedditScrapeService


class RedditScraperOrchestrator:
    def __init__(self, sites_file: str = "sites_to_scrape.txt"):
        self.sites_file = sites_file
        self.reddit_service = RedditScrapeService()
        self.output_dir = "output"
        
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


if __name__ == "__main__":
    orchestrator = RedditScraperOrchestrator()
    
    # Scrape all sites from the file
    orchestrator.scrape_all_sites(limit_per_site=10)
    
    # Or scrape a single site
    # orchestrator.scrape_single_site("https://www.reddit.com/r/CryptoCurrency/", limit=20)