# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Reddit scraping and data processing pipeline that collects cryptocurrency and technology-related posts from Reddit, processes them through NLP analysis, and streams the data to Kafka for real-time analysis. The project includes sentiment analysis, language detection, and entity extraction capabilities.

## Architecture

The system consists of several main components:

### Core Components
- **Reddit Scraper** (`data_scraping/`): Web scraping service that collects posts from Reddit subreddits using unstructured.io for HTML processing
- **Scheduler** (`cronjob/`): APScheduler-based system that runs scraping at different intervals (5m, 15m, 1h, 4h)
- **Kafka Producer** (`kafka/`): Streams processed Reddit data to Kafka topics
- **Main Orchestration** (`main.py`): Coordinates the scheduler and producer integration

### Data Processing
- **Language Detection** (`utils/lang_detect.py`): Filters non-English content using langdetect
- **Data Pipeline**: Raw Reddit posts → Cleaning → Language detection → Entity extraction → Kafka streaming

### Storage & Output
- **JSON Files**: Raw and processed data stored in structured JSON format with timestamps
- **Kafka Topics**: Raw data sent to `reddit-raw` topic for further processing
- **Output Directories**: `data/reddit_posts/`, `output_reddit/` contain scraped data

## Key Commands

### Development Setup
```bash
# Install dependencies using uv (recommended)
uv sync

# Alternative with pip
pip install -e .
```

### Running the System
```bash
# Main orchestration with Kafka integration
python main.py

# Run individual components
cd cronjob && python reddit_scheduler.py      # Scheduler only
cd data_scraping && python reddit_scraper.py   # Scraper only
cd kafka && python reddit_producer.py         # Producer only
```

### Docker Setup
```bash
# Start Kafka service
docker-compose up -d kafka

# View Kafka logs
docker-compose logs kafka
```

### Configuration
```bash
# Configure subreddits to scrape
# Edit: cronjob/sites_to_scrape.txt or cronjob/sites_to_scrape.txt
# Format: https://www.reddit.com/r/subreddit/

# Check scheduler configuration
# cronjob/reddit_scheduler.py:32-50
```

## Data Flow

1. **Scheduler** triggers scraping at configured intervals
2. **Reddit Scraper** collects posts from specified subreddits
3. **Language Detection** filters non-English content
4. **Data Processing** includes cleaning, entity extraction, and metadata enrichment
5. **Kafka Producer** streams processed data to Kafka topics
6. **Output** saved as JSON files with timestamps

## Configuration Files

### Sites Configuration
- `cronjob/sites_to_scrape.txt`: List of Reddit URLs to scrape
- Default subreddits: CryptoCurrency, Bitcoin, Ethereum, CryptoMarkets, technology

### Kafka Configuration
- Bootstrap servers: `localhost:9092` (default)
- Topic: `reddit-raw`
- Configuration in `kafka/reddit_producer.py:20-21`

### Logging
- System logs to console with timestamps
- Scheduler logs: `cronjob/logs/reddit_scheduler.log`
- Color logging enabled for better visibility

## Important Implementation Details

### Rate Limiting
- Reddit scraper implements 3-second base delay between requests
- Exponential backoff with random jitter (0.5-1.5x base delay)
- Maximum retries: 3 attempts per request
- User-agent rotation to avoid detection

### Data Processing
- Author information anonymized using SHA256 hashing
- Text cleaning and entity extraction using unstructured.io
- Language detection using langdetect library
- Timestamp-based file naming to prevent overwrites

### Error Handling
- Graceful degradation from JSON API to HTML parsing
- Comprehensive logging for debugging
- Kafka producer handles connection errors and retries

## Testing

### Manual Testing
- Test individual components separately using the commands above
- Monitor logs for errors and performance metrics
- Verify JSON output structure and data quality

### Integration Testing
- Run the full pipeline and verify data flows correctly
- Check Kafka topic for messages
- Verify output files contain expected data structure

## Dependencies

### Core Dependencies (from pyproject.toml)
- `apscheduler>=3.10.0`: Task scheduling
- `kafka-python>=2.2.15`: Kafka integration
- `unstructured>=0.10.0`: HTML/text processing
- `langdetect>=1.0.9`: Language detection
- `requests>=2.28.0`: HTTP requests
- `colorlog>=6.7.0`: Enhanced logging

### Python Requirements
- Python >= 3.13 (specified in pyproject.toml)
- uv preferred for dependency management

## File Structure Highlights

### Key Directories
- `data_scraping/`: Core scraping logic
- `cronjob/`: Scheduling and orchestration
- `kafka/`: Kafka integration
- `utils/`: Utility functions (language detection)
- `colab/`: Jupyter notebooks for analysis
- `output_reddit/`: Processed data output

### Configuration Files
- `pyproject.toml`: Project dependencies and metadata
- `docker-compose.yaml`: Kafka service configuration
- `.gitignore`: Version control exclusions
- `sites_to_scrape.txt`: Scraping targets configuration