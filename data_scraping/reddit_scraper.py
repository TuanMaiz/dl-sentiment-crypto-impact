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
    
    def scrape_subreddit(self, subreddit_url: str, limit: int = 25, include_comments: bool = False) -> List[Dict[str, Any]]:
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
                    structured_post = self._structure_reddit_post(post, include_comments)
                    if structured_post:
                        posts.append(structured_post)
                        
        except Exception as e:
            print(f"Error scraping subreddit {subreddit_url}: {str(e)}")
            # Fallback to HTML scraping if JSON fails
            posts = self._scrape_subreddit_html(subreddit_url, limit)
        
        return posts
    
    def scrape_posts_by_date_range(self, subreddit_url: str, start_date: str, end_date: str, limit: int = 100, include_comments: bool = False) -> List[Dict[str, Any]]:
        """
        Scrape Reddit posts from a subreddit within a specific date range
        
        Args:
            subreddit_url: URL of the subreddit to scrape (e.g., "https://www.reddit.com/r/CryptoCurrency/")
            start_date: Start date in format "YYYY-MM-DD" or Unix timestamp
            end_date: End date in format "YYYY-MM-DD" or Unix timestamp
            limit: Maximum number of posts to scrape
            include_comments: Whether to include comments for each post
            
        Returns:
            List of structured post data within the date range
        """
        posts = []
        
        try:
            # Parse dates
            start_timestamp = self._parse_date(start_date)
            end_timestamp = self._parse_date(end_date)
            
            # Extract subreddit name from URL
            subreddit_name = self._extract_subreddit_name(subreddit_url)
            
            # Try different search query formats
            search_queries = [
                f'timestamp:{start_timestamp}..{end_timestamp}',
                f'{start_timestamp}..{end_timestamp}',
                f'created:{start_timestamp}..{end_timestamp}'
            ]
            
            search_url = f"https://www.reddit.com/r/{subreddit_name}/search.json"
            
            for query in search_queries:
                params = {
                    'q': query,
                    'sort': 'new',
                    't': 'all',
                    'limit': min(limit, 100)  # Reddit API limit is 100 per request
                }
                
                try:
                    response = requests.get(search_url, headers=self.headers, params=params, timeout=15)
                    if response.status_code == 200:
                        data = response.json()
                        
                        if 'data' in data and 'children' in data['data']:
                            for post_data in data['data']['children']:
                                post = post_data['data']
                                
                                # Verify post is within date range
                                post_timestamp = post.get('created_utc', 0)
                                if start_timestamp <= post_timestamp <= end_timestamp:
                                    structured_post = self._structure_reddit_post(post, include_comments)
                                    if structured_post:
                                        posts.append(structured_post)
                        
                        # If we found results, don't try other queries
                        if posts:
                            break
                
                except Exception as e:
                    print(f"Search query '{query}' failed: {e}")
                    continue
            
            # If timestamp search didn't work, fall back to getting recent posts and filtering
            if not posts:
                print("Timestamp search failed, falling back to recent posts filtering...")
                recent_posts = self.scrape_subreddit(subreddit_url, limit * 2, include_comments)  # Get more posts to filter
                
                for post in recent_posts:
                    try:
                        post_timestamp = datetime.fromisoformat(post['post_created_timestamp'].replace('Z', '+00:00')).timestamp()
                        if start_timestamp <= post_timestamp <= end_timestamp:
                            posts.append(post)
                            if len(posts) >= limit:
                                break
                    except Exception as e:
                        print(f"Error filtering post by timestamp: {e}")
                        continue
            
            # Pagination is now handled in the search loop above
            
        except Exception as e:
            print(f"Error scraping posts by date range: {str(e)}")
            # Fallback to regular scraping and filter by date
            all_posts = self.scrape_subreddit(subreddit_url, limit * 2, include_comments)  # Get more posts to filter
            start_timestamp = self._parse_date(start_date)
            end_timestamp = self._parse_date(end_date)
            
            for post in all_posts:
                try:
                    post_timestamp = datetime.fromisoformat(post['post_created_timestamp'].replace('Z', '+00:00')).timestamp()
                    if start_timestamp <= post_timestamp <= end_timestamp:
                        posts.append(post)
                        if len(posts) >= limit:
                            break
                except Exception as e:
                    print(f"Error filtering post by timestamp: {e}")
                    continue
        
        return posts[:limit]
    
    def _parse_date(self, date_input: str) -> float:
        """Parse date string or timestamp to Unix timestamp"""
        try:
            # Check if it's already a timestamp
            if date_input.replace('.', '').isdigit():
                return float(date_input)
            
            # Parse YYYY-MM-DD format
            if '-' in date_input:
                dt = datetime.strptime(date_input, '%Y-%m-%d')
                return dt.replace(tzinfo=timezone.utc).timestamp()
            
            # Try other common formats
            formats = ['%Y-%m-%d %H:%M:%S', '%Y/%m/%d', '%d/%m/%Y', '%m/%d/%Y']
            for fmt in formats:
                try:
                    dt = datetime.strptime(date_input, fmt)
                    return dt.replace(tzinfo=timezone.utc).timestamp()
                except ValueError:
                    continue
                    
            raise ValueError(f"Unable to parse date: {date_input}")
            
        except Exception as e:
            print(f"Error parsing date '{date_input}': {str(e)}")
            return 0.0
    
    def _extract_subreddit_name(self, subreddit_url: str) -> str:
        """Extract subreddit name from URL"""
        try:
            # Handle various URL formats
            if 'reddit.com/r/' in subreddit_url:
                parts = subreddit_url.split('reddit.com/r/')[1].split('/')[0]
                return parts
            elif subreddit_url.startswith('r/'):
                return subreddit_url[2:]
            else:
                return subreddit_url.strip('/')
        except Exception:
            return subreddit_url
    
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
                        structured_post = self._create_post_from_extracted(current_post, subreddit_url, include_comments)
                        if structured_post:
                            posts.append(structured_post)
                        current_post = {}
                        if len(posts) >= limit:
                            break
                            
        except Exception as e:
            print(f"Error in HTML scraping: {str(e)}")
        
        return posts
    
    def scrape_post_comments(self, post_permalink: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Scrape comments for a specific Reddit post
        
        Args:
            post_permalink: The permalink of the post (e.g., "/r/CryptoCurrency/comments/abc123/")
            limit: Maximum number of comments to scrape
            
        Returns:
            List of structured comment data
        """
        comments = []
        
        try:
            # Add .json to the permalink to get JSON response
            if not post_permalink.endswith('/'):
                post_permalink += '/'
            
            comments_url = f"https://www.reddit.com{post_permalink}.json"
            
            response = requests.get(comments_url, headers=self.headers, timeout=15)
            
            # Handle rate limiting
            if response.status_code == 429:
                print(f"Rate limit hit for {post_permalink}, skipping comment scraping")
                return comments
            
            response.raise_for_status()
            
            data = response.json()
            
            # The first element is the post data, the second is comments data
            if len(data) >= 2 and 'data' in data[1] and 'children' in data[1]['data']:
                for comment_data in data[1]['data']['children'][:limit]:
                    comment = comment_data['data']
                    
                    # Skip deleted/removed comments
                    if comment.get('author') == '[deleted]' or comment.get('body') == '[removed]':
                        continue
                    
                    structured_comment = {
                        "id": f"reddit_comment_{comment.get('id', '')}",
                        "platform": "reddit",
                        "type": "comment",
                        "author_id": self._hash_author(comment.get('author', '')) if comment.get('author') else None,
                        "text_clean": self._clean_text(comment.get('body', '')),
                        "lang": 'en',
                        "comment_created_timestamp": datetime.fromtimestamp(
                            comment.get('created_utc', 0), 
                            tz=timezone.utc
                        ).isoformat(),
                        "post_permalink": post_permalink,
                        "score": comment.get('score', 0),
                        "is_submitter": comment.get('is_submitter', False),
                        "reply_count": comment.get('count', 0)
                    }
                    
                    comments.append(structured_comment)
            
            # Rate limiting delay
            time.sleep(0.5)
        
        except Exception as e:
            print(f"Error scraping comments for {post_permalink}: {str(e)}")
        
        return comments
    
    def _structure_reddit_post(self, post_data: Dict, include_comments: bool = False) -> Optional[Dict[str, Any]]:
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
            
            post_permalink = post_data.get('permalink', '')
            post_metadata = {
                "source_url": f"https://www.reddit.com{post_permalink}",
                "subreddit": post_data.get('subreddit', ''),
                "score": post_data.get('score', 0),
                "upvote_ratio": post_data.get('upvote_ratio', 0),
                "num_comments": post_data.get('num_comments', 0),
                "post_type": post_data.get('post_hint', 'text')
            }
            
            # Include comments if requested
            comments = []
            if include_comments and post_permalink:
                comments = self.scrape_post_comments(post_permalink, limit=50)
            
            structured_post = {
                "id": f"reddit_{post_data.get('id', '')}",
                "platform": "reddit",
                "author_id": author_id,
                "text_clean": text_clean,
                "lang": post_data.get('lang', 'en'),
                "post_created_timestamp": datetime.fromtimestamp(
                    post_data.get('created_utc', time.time()), 
                    tz=timezone.utc
                ).isoformat(),
                "keywords": keywords,
                "entities": entities,
                "comments": comments,
                "comment_count": len(comments),
                "meta": post_metadata,
                "scraped_at_timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            return structured_post
            
        except Exception as e:
            print(f"Error structuring post: {str(e)}")
            return None
    
    def _create_post_from_extracted(self, extracted: Dict, source_url: str, include_comments: bool = False) -> Optional[Dict[str, Any]]:
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
                "post_created_timestamp": datetime.now(timezone.utc).isoformat(),
                "keywords": self._extract_keywords(text_clean),
                "entities": self._extract_entities(text_clean),
                "comments": [],
                "comment_count": 0,
                "meta": {
                    "source_url": source_url,
                    "extraction_method": "html_fallback"
                },
                "scraped_at_timestamp": datetime.now(timezone.utc).isoformat()
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