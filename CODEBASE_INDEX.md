# Codebase Index - DL Sentiment Crypto Impact

**Project Overview:** A Python-based Reddit scraper designed for cryptocurrency sentiment analysis with distributed computing capabilities using Apache Flink.

**Last Updated:** October 13, 2025
**Python Version:** 3.13
**Main Technologies:** Python, Apache Flink, Docker, unstructured.io

---

## 📁 Directory Structure

```
dl-sentiment-crypto-impact/
├── .git/                           # Git repository metadata
├── .gitignore                      # Git ignore rules (Python, venv)
├── .python-version                 # Python version specification (3.13)
├── README.md                       # Project documentation
├── main.py                         # Simple entry point (Hello World)
├── pyproject.toml                  # Python project configuration
├── uv.lock                         # UV dependency lock file
├── docker-compose.yml              # Flink cluster orchestration
├── dockerfile                      # PyFlink container setup
├── apache_flink-2.1.0.tar          # Flink runtime distribution
├── apache_flink_libraries-2.1.0.tar # Flink libraries
├── data_scraping/                  # Reddit scraping module
│   ├── main_scraper.py             # Scraping orchestrator
│   ├── reddit_scraper.py           # Reddit scraping service
│   ├── sites_to_scrape.txt         # List of subreddits to scrape
│   └── output/                     # Scraped data output directory
│       └── reddit_posts_20251007_225355.json # Sample scraped data
└── main/                           # Main processing modules
    ├── data_scraping/              # Symlink to main data_scraping (if needed)
    └── flink_processing/           # PyFlink MongoDB ingestion module
        ├── mongodb_ingestion.py    # Main PyFlink ingestion script
        ├── config.yml              # Configuration settings
        ├── requirements.txt        # Python dependencies
        ├── .env.example           # Environment variables template
        ├── README.md               # Module documentation
        └── run_ingestion.sh       # Pipeline runner script
```

---

## 🗂️ File Index

### Configuration Files

| File | Purpose | Key Details |
|------|---------|-------------|
| `.python-version` | Python version specification | Version: 3.13 |
| `.gitignore` | Git ignore patterns | Excludes: `__pycache__/`, `*.py[oc]`, build artifacts, `.venv` |
| `pyproject.toml` | Python project metadata | Dependencies: `requests>=2.32.5`, `unstructured>=0.18.15` |
| `uv.lock` | Dependency lock file | UV package manager lock (149KB) |

### Docker & Infrastructure

| File | Purpose | Key Details |
|------|---------|-------------|
| `docker-compose.yml` | Flink cluster setup | Services: jobmanager (port 8085), taskmanager (2 slots) |
| `dockerfile` | PyFlink container | Base: flink:2.1.0, Python 3.9.8, Java 11 |
| `apache_flink-2.1.0.tar` | Flink runtime | Size: ~15MB |
| `apache_flink_libraries-2.1.0.tar` | Flink libraries | Size: ~311MB |

### Core Application

| File | Purpose | Key Details |
|------|---------|-------------|
| `main.py` | Entry point | Simple "Hello World" placeholder |
| `README.md` | Project documentation | Comprehensive setup and usage guide |

### Data Scraping Module (`data_scraping/`)

| File | Purpose | Key Details |
|------|---------|-------------|
| `main_scraper.py` | Scraping orchestrator | Class: `RedditScraperOrchestrator`, manages multi-site scraping |
| `reddit_scraper.py` | Reddit service | Class: `RedditScrapeService`, handles JSON/HTML scraping |
| `sites_to_scrape.txt` | Target subreddits | Currently: r/CryptoCurrency, r/CryptoMarkets |

### PyFlink Processing Module (`flink_processing/`)

| File | Purpose | Key Details |
|------|---------|-------------|
| `mongodb_ingestion.py` | PyFlink MongoDB pipeline | Main ingestion script with distributed processing |
| `config.yml` | Configuration file | MongoDB, PyFlink, and pipeline settings |
| `requirements.txt` | Dependencies | PyMongo, PyYAML, python-dotenv, etc. |
| `.env.example` | Environment template | MongoDB connection and configuration variables |
| `README.md` | Module documentation | Comprehensive setup and usage guide |
| `run_ingestion.sh` | Pipeline runner | Automated script for running ingestion pipeline |

---

## 🔧 Core Components

### 1. RedditScraperOrchestrator (`main_scraper.py`)
- **Purpose:** Coordinates scraping across multiple subreddits
- **Key Methods:**
  - `load_sites_to_scrape()`: Loads subreddit URLs from file
  - `scrape_all_sites()`: Scrapes all configured subreddits
  - `scrape_single_site()`: Scrapes individual subreddit
- **Output:** Timestamped JSON files in `output/` directory

### 2. RedditScrapeService (`reddit_scraper.py`)
- **Purpose:** Handles Reddit data extraction and processing
- **Features:**
  - JSON API scraping (preferred method)
  - HTML fallback scraping using unstructured.io
  - Data cleaning and normalization
  - Author anonymization (SHA256 hashing)
  - Keyword extraction (crypto-focused)
  - Basic named entity recognition
- **Output Format:**
  ```json
  {
    "id": "reddit_post_id",
    "platform": "reddit",
    "author_id": "hashed_author_id",
    "text_clean": "cleaned post content",
    "lang": "en",
    "timestamp": "ISO8601",
    "keywords": ["bitcoin", "crypto"],
    "entities": [{"type": "ORG", "text": "CompanyName"}],
    "meta": {
      "source_url": "reddit_url",
      "subreddit": "CryptoCurrency",
      "score": 100,
      "upvote_ratio": 0.85,
      "num_comments": 25,
      "post_type": "link"
    }
  }
  ```

### 3. PyFlink MongoDB Pipeline (`flink_processing/mongodb_ingestion.py`)
- **Purpose:** Distributed data ingestion from JSON files to MongoDB
- **Key Classes:**
  - `RedditToMongoDBPipeline`: Main pipeline orchestrator
  - `MongoDBSink`: MongoDB output sink with upsert operations
  - `JSONFileReader`: JSON file parsing and validation
- **Features:**
  - Parallel processing with configurable parallelism
  - Automatic schema validation and data cleaning
  - MongoDB indexing for performance optimization
  - Fault tolerance with checkpointing
  - Comprehensive error handling and logging

### 4. Infrastructure (Flink + Docker)
- **Flink Version:** 2.1.0
- **Python Integration:** PyFlink with Python 3.9.8
- **Architecture:** JobManager + TaskManager setup
- **Scaling:** Configurable task slots (default: 2)

---

## 📊 Data Flow

1. **Data Collection:**
   - Input: Subreddit URLs from `sites_to_scrape.txt`
   - Processing: JSON API extraction (primary), HTML parsing fallback
   - Cleaning: Text normalization, keyword/entity extraction, author anonymization
   - Output: Structured JSON files in `data_scraping/output/`

2. **Data Ingestion (PyFlink Pipeline):**
   - Input: JSON files from scraping output
   - Processing: Distributed PyFlink pipeline with parallel processing
   - Validation: Schema validation and data quality checks
   - Transformation: Add processing metadata and timestamps
   - Storage: MongoDB with automatic indexing and upsert operations

---

## 🎯 Current Capabilities

### ✅ Implemented Features
- Multi-subreddit scraping orchestration
- Reddit JSON API integration with HTML fallback
- Text cleaning and normalization
- Crypto-focused keyword extraction
- Basic named entity recognition
- Author privacy protection (SHA256 hashing)
- Structured JSON output with timestamps
- **PyFlink distributed processing pipeline**
- **MongoDB ingestion with automatic indexing**
- **Schema validation and data quality checks**
- **Fault-tolerant processing with checkpointing**
- Docker containerization for Flink cluster
- Comprehensive error handling and logging
- Configurable environment and YAML settings

### 🔄 In Progress/Planned
- Advanced NLP for sentiment analysis
- Real-time streaming capabilities
- Additional database storage options (Elasticsearch, PostgreSQL)
- Twitter/X scraping support
- Web-based monitoring dashboard

---

## 🚀 Quick Start Commands

```bash
# Setup environment
uv sync

# Basic scraping
cd data_scraping
python main_scraper.py

# Docker setup for Flink cluster
docker build --tag pyflink:2.1.0 .
docker compose up -d

# MongoDB ingestion pipeline
cd flink_processing
cp .env.example .env  # Configure your MongoDB settings
./run_ingestion.sh --install-deps --yes

# Or run ingestion directly
python mongodb_ingestion.py

# Single subreddit scraping
python -c "
from main_scraper import RedditScraperOrchestrator
orchestrator = RedditScraperOrchestrator()
orchestrator.scrape_single_site('https://www.reddit.com/r/CryptoCurrency/', limit=20)
"
```

---

## 🔐 Privacy & Security

- **Author Anonymization:** SHA256 hashing of usernames
- **Data Cleaning:** URL removal, text normalization
- **Rate Limiting:** Respectful scraping practices
- **Error Handling:** Graceful degradation to HTML parsing

---

## 📈 Sample Data

**Sample Output File:** `reddit_posts_20251007_225355.json` (24KB)
- Contains structured Reddit post data
- Cryptocurrency-focused content from r/CryptoCurrency and r/CryptoMarkets

---

## 🐛 Technical Debt & Improvements

1. **Main.py:** Currently just a placeholder - needs main application logic
2. **NLP Enhancement:** Basic keyword extraction could be improved with spaCy/NLTK
3. **Stream Processing:** Current pipeline is batch-based, needs real-time streaming
4. **Data Validation:** Could benefit from Pydantic models for stronger typing
5. **Monitoring:** Needs metrics collection and alerting system
6. **Testing:** Comprehensive unit and integration tests needed
7. **Configuration:** Could use more sophisticated configuration management

---

## 📚 Dependencies

**Core Dependencies:**
- `requests>=2.32.5`: HTTP client for Reddit API
- `unstructured>=0.18.15`: Document processing and HTML parsing
- `pymongo>=4.3.3`: MongoDB driver for data ingestion
- `PyYAML>=6.0`: Configuration file processing
- `python-dotenv>=1.0.0`: Environment variable management

**System Dependencies:**
- Python 3.13 (development), Python 3.9.8 (Flink container)
- Java 11 (for Apache Flink)
- Apache Flink 2.1.0 with PyFlink
- MongoDB 4.0+ (local or remote)
- Docker & Docker Compose

---

*This index was generated automatically on October 13, 2025. For the most current information, refer to the source code and documentation.*