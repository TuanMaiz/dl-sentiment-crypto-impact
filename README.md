# Reddit Scraper

A Python-based Reddit scraper using unstructured.io for data extraction and processing.

## Features

- Scrapes Reddit posts from specified subreddits
- Extracts structured data including:
  - Post ID and platform
  - Anonymized author information
  - Cleaned text content
  - Language detection
  - Timestamps
  - Keywords and entities
  - Metadata (score, comments, etc.)
- Saves data in structured JSON format

## Installation

1. Clone or download this repository
2. Install dependencies using uv:

```bash
uv sync
```

3. Setup flink container

**Go to main/flink_processing/README.md**

## Usage

### Basic Usage

Run the scraper to collect posts from all sites listed in `sites_to_scrape.txt`:

```bash
cd data_scraping
python main_scraper.py
```

### Scrape a Single Subreddit

```python
from main_scraper import RedditScraperOrchestrator

orchestrator = RedditScraperOrchestrator()
orchestrator.scrape_single_site("https://www.reddit.com/r/CryptoCurrency/", limit=20)
```

### Configuration

1. **Add subreddits to scrape**: Edit `sites_to_scrape.txt` and add Reddit URLs (one per line):

```
https://www.reddit.com/r/CryptoCurrency/
https://www.reddit.com/r/CryptoMarkets/
https://www.reddit.com/r/technology/
```

2. **Adjust scraping limits**: Modify the `limit` parameter to control how many posts to scrape per subreddit.

## Output Structure

The scraper generates JSON files with the following structure:

```json
{
  "id": "reddit_post_id",
  "platform": "reddit",
  "author_id": "hashed_author_id",
  "text_clean": "cleaned post content",
  "lang": "en",
  "timestamp": "2024-01-01T12:00:00Z",
  "keywords": ["bitcoin", "crypto", "trading"],
  "entities": [
    {
      "type": "ORG",
      "text": "CompanyName"
    }
  ],
  "meta": {
    "source_url": "https://www.reddit.com/r/subreddit/post_id/",
    "subreddit": "CryptoCurrency",
    "score": 100,
    "upvote_ratio": 0.85,
    "num_comments": 25,
    "post_type": "link"
  }
}
```

## Files Structure

```
data_scraping/
├── main_scraper.py      # Main orchestrator script
├── reddit_scraper.py    # Reddit scraping service
├── sites_to_scrape.txt  # List of URLs to scrape
└── output/              # Directory for scraped JSON files
```

### Overview structure (new)

```
main/
├── data_scraping/
│   ├── reddit_scraper.py
│   ├── main_scraper.py
│   ├── sites_to_scrape.txt
│   └── output/
│       └── reddit_posts_*.json
│
└── flink_processing/
    ├── mongodb_ingestion.py              # Bronze layer ingestion
    ├── data_cleaning_operator.py         # Silver layer cleaning
    ├── sentiment_aggregation_operator.py # Gold layer pre-aggregation
    ├── __init__.py
    ├── config.yml
    ├── .env
    └── run_ingestion.sh
```

## Dependencies

- `unstructured`: For HTML and text processing
- `requests`: For HTTP requests

## Notes

- The scraper respects Reddit's rate limits
- Author information is anonymized using SHA256 hashing
- Posts are saved with timestamps to prevent overwriting
- Error handling includes fallback to HTML parsing if JSON API fails

## Future Enhancements

- Twitter/X scraping support
- Advanced NLP for keyword/entity extraction
- Database storage options
- Real-time streaming capabilities
