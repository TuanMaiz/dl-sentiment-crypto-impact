import json
import hashlib
import re
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from unstructured.partition.html import partition_html
from unstructured.partition.auto import partition
import requests
from urllib.parse import urljoin, urlparse
import time


class RedditScrapeService:
    def __init__(self):
        self.base_url = "https://www.reddit.com"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
    
    def scrape_subreddit(self, subreddit_url: str, limit: int = 25) -> List[Dict[str, Any]]:
        """
        Scrape posts from a subreddit URL
        
        Args:
            subreddit_url: URL of the subreddit to scrape
            limit: Maximum number of posts to scrape
            
        Returns:
            List of structured post data
        """
        posts = []
        
        try:
            # Add .json to the URL to get JSON response
            if subreddit_url.endswith('/'):
                json_url = subreddit_url + '.json'
            else:
                json_url = subreddit_url + '/.json'
            
            response = requests.get(json_url, headers=self.headers, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            # Handle Reddit's JSON structure
            if 'data' in data and 'children' in data['data']:
                for i, post_data in enumerate(data['data']['children'][:limit]):
                    if i >= limit:
                        break
                        
                    post = post_data['data']
                    structured_post = self._structure_reddit_post(post)
                    if structured_post:
                        posts.append(structured_post)
                        
        except Exception as e:
            print(f"Error scraping subreddit {subreddit_url}: {str(e)}")
            # Fallback to HTML scraping if JSON fails
            posts = self._scrape_subreddit_html(subreddit_url, limit)
        
        return posts
    
    def _scrape_subreddit_html(self, subreddit_url: str, limit: int = 25) -> List[Dict[str, Any]]:
        """Fallback HTML scraping method"""
        posts = []
        
        try:
            response = requests.get(subreddit_url, headers=self.headers, timeout=10)
            response.raise_for_status()
            
            # Use unstructured to parse HTML
            elements = partition(html=response.content)
            
            # Extract post information from parsed elements
            current_post = {}
            for element in elements:
                text = str(element).strip()
                if text and len(text) > 20:  # Filter out short elements
                    if 'title' not in current_post:
                        current_post['title'] = text
                    elif 'selftext' not in current_post:
                        current_post['selftext'] = text
                    elif current_post:
                        # Create a structured post from extracted data
                        structured_post = self._create_post_from_extracted(current_post, subreddit_url)
                        if structured_post:
                            posts.append(structured_post)
                        current_post = {}
                        if len(posts) >= limit:
                            break
                            
        except Exception as e:
            print(f"Error in HTML scraping: {str(e)}")
        
        return posts
    
    def _structure_reddit_post(self, post_data: Dict) -> Optional[Dict[str, Any]]:
        """Structure Reddit post data according to the specified format"""
        try:
            # Extract and clean text
            title = post_data.get('title', '')
            selftext = post_data.get('selftext', '')
            text_clean = f"{title} {selftext}".strip()
            
            # Clean text (remove URLs, normalize)
            text_clean = self._clean_text(text_clean)
            
            # Generate anonymized author ID
            author = post_data.get('author', '')
            author_id = self._hash_author(author) if author else None
            
            # Extract keywords (simple approach - can be enhanced)
            keywords = self._extract_keywords(text_clean)
            
            # Extract entities (basic implementation)
            entities = self._extract_entities(text_clean)
            
            structured_post = {
                "id": f"reddit_{post_data.get('id', '')}",
                "platform": "reddit",
                "author_id": author_id,
                "text_clean": text_clean,
                "lang": post_data.get('lang', 'en'),
                "timestamp": datetime.fromtimestamp(
                    post_data.get('created_utc', time.time()), 
                    tz=timezone.utc
                ).isoformat(),
                "keywords": keywords,
                "entities": entities,
                "meta": {
                    "source_url": f"https://www.reddit.com{post_data.get('permalink', '')}",
                    "subreddit": post_data.get('subreddit', ''),
                    "score": post_data.get('score', 0),
                    "upvote_ratio": post_data.get('upvote_ratio', 0),
                    "num_comments": post_data.get('num_comments', 0),
                    "post_type": post_data.get('post_hint', 'text')
                }
            }
            
            return structured_post
            
        except Exception as e:
            print(f"Error structuring post: {str(e)}")
            return None
    
    def _create_post_from_extracted(self, extracted: Dict, source_url: str) -> Optional[Dict[str, Any]]:
        """Create structured post from extracted HTML data"""
        try:
            title = extracted.get('title', '')
            selftext = extracted.get('selftext', '')
            text_clean = f"{title} {selftext}".strip()
            
            if not text_clean:
                return None
                
            # Generate a unique ID from hash
            post_id = hashlib.md5(text_clean.encode()).hexdigest()[:12]
            
            structured_post = {
                "id": f"reddit_html_{post_id}",
                "platform": "reddit",
                "author_id": None,
                "text_clean": self._clean_text(text_clean),
                "lang": "en",  # Default, could be detected
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "keywords": self._extract_keywords(text_clean),
                "entities": self._extract_entities(text_clean),
                "meta": {
                    "source_url": source_url,
                    "extraction_method": "html_fallback"
                }
            }
            
            return structured_post
            
        except Exception as e:
            print(f"Error creating post from extracted: {str(e)}")
            return None
    
    def _clean_text(self, text: str) -> str:
        """Clean and normalize text"""
        # Remove URLs
        text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text)
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        
        # Normalize (basic approach)
        text = text.lower()
        
        return text
    
    def _hash_author(self, author: str) -> str:
        """Hash author name for privacy"""
        return hashlib.sha256(author.encode()).hexdigest()[:16]
    
    def _extract_keywords(self, text: str) -> List[str]:
        """Extract keywords from text (simple implementation)"""
        # Simple keyword extraction - can be enhanced with NLP libraries
        crypto_keywords = [
            'bitcoin', 'btc', 'ethereum', 'eth', 'crypto', 'cryptocurrency',
            'blockchain', 'defi', 'nft', 'altcoin', 'token', 'coin',
            'trading', 'investment', 'price', 'market', 'bull', 'bear'
        ]
        
        text_lower = text.lower()
        found_keywords = []
        
        for keyword in crypto_keywords:
            if keyword in text_lower:
                found_keywords.append(keyword)
        
        return list(set(found_keywords))
    
    def _extract_entities(self, text: str) -> List[Dict[str, str]]:
        """Extract named entities (basic implementation)"""
        entities = []
        
        # Simple regex patterns for common entities
        # Can be enhanced with spaCy or similar
        
        # Look for capitalized words (potential organizations/people)
        words = text.split()
        for word in words:
            if word.istitle() and len(word) > 3:
                entities.append({
                    "type": "ORG",
                    "text": word
                })
        
        return entities[:5]  # Limit to prevent too many entities
    
    def save_to_json(self, posts: List[Dict[str, Any]], filename: str) -> None:
        """Save posts to JSON file"""
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(posts, f, indent=2, ensure_ascii=False)