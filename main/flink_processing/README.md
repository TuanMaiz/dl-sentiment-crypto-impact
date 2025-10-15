# PyFlink MongoDB Ingestion Module

This module provides distributed data ingestion from Reddit JSON files to MongoDB using Apache Flink and PyFlink.

## 🎯 Overview

The PyFlink MongoDB ingestion pipeline reads structured Reddit post data (JSON format) and efficiently loads it into a MongoDB database with the following features:

- **Distributed Processing**: Uses Apache Flink for scalable, fault-tolerant processing
- **Duplicate Handling**: Implements upsert operations to prevent data duplication
- **Schema Validation**: Validates Reddit post structure before insertion
- **Performance Optimization**: Automatic index creation for optimal query performance
- **Monitoring**: Comprehensive logging and job monitoring
- **Configuration**: Flexible configuration via environment variables and YAML

## 📁 Module Structure

```
main/flink_processing/
├── mongodb_ingestion.py     # Main PyFlink ingestion script
├── config.yml              # Configuration settings
├── requirements.txt         # Python dependencies
├── .env.example            # Environment variables template
├── README.md               # This documentation
└── run_ingestion.sh        # Pipeline runner script
```

## 🚀 Quick Start

### 1. Prerequisites

- MongoDB server running (local or remote)
- Apache Flink 2.1.0 with PyFlink installed
- Python 3.9+ (compatible with PyFlink)
- Reddit data JSON file (from data_scraping module)

### 2. Setup Environment

```bash
# Navigate to flink_processing directory
cd main/flink_processing

# Copy environment template
cp .env.example .env

# Edit .env with your MongoDB settings
nano .env
```

### 3. Configure MongoDB

Update `.env` file with your MongoDB settings:

### 4. Run the Ingestion Pipeline

#### Option A: Direct Python Execution

```bash
# Install dependencies (if not in Flink container)
pip install -r requirements.txt

# Run the ingestion script
python mongodb_ingestion_operator.py
```

#### Option B: Using Docker with Flink Cluster

Download 2 files from:

- "https://pypi.org/project/apache-flink/"

- "https://pypi.org/project/apache-flink-libraries/"

```bash
# From project root directory
docker compose up -d

# Submit job to Flink cluster
docker exec -it <jobmanager_container> python /home/pyflink/main/flink_processing/mongodb_ingestion.py
```

## 🔧 Configuration

### Environment Variables

| Variable             | Default                                                      | Description                 |
| -------------------- | ------------------------------------------------------------ | --------------------------- |
| `MONGODB_HOST`       | localhost                                                    | MongoDB server hostname     |
| `MONGODB_PORT`       | 27017                                                        | MongoDB server port         |
| `MONGODB_DATABASE`   | crypto_sentiment                                             | Target database name        |
| `MONGODB_COLLECTION` | reddit_posts                                                 | Target collection name      |
| `MONGODB_USERNAME`   | null                                                         | MongoDB username (optional) |
| `MONGODB_PASSWORD`   | null                                                         | MongoDB password (optional) |
| `FLINK_PARALLELISM`  | 2                                                            | PyFlink job parallelism     |
| `INPUT_FILE_PATH`    | ../../data_scraping/output/reddit_posts_20251007_225355.json | Input JSON file path        |
| `LOG_LEVEL`          | INFO                                                         | Logging level               |

### Configuration File (config.yml)

The `config.yml` file provides additional configuration options:

- MongoDB connection settings and timeouts
- PyFlink job configuration
- Input validation settings
- Logging configuration
- Output processing options

## 📊 Data Flow

```mermaid
graph LR
    A[JSON File] --> B[PyFlink Reader]
    B --> C[Schema Validation]
    C --> D[Data Transformation]
    D --> E[MongoDB Sink]
    E --> F[Index Creation]
    F --> G[MongoDB Collection]
```

1. **Input**: Reddit JSON files from `../../data_scraping/output/`
2. **Processing**: PyFlink pipeline with parallel processing
3. **Validation**: Schema validation for data quality
4. **Transformation**: Add processing metadata and timestamps
5. **Output**: MongoDB collection with optimized indexes

## 🗄️ MongoDB Schema

The ingested documents follow this structure:

```json
{
  "_id": "ObjectId(...)",
  "id": "reddit_post_12345",
  "platform": "reddit",
  "author_id": "hashed_author_id",
  "text_clean": "cleaned post content",
  "lang": "en",
  "timestamp": "2024-10-07T22:53:55Z",
  "keywords": ["bitcoin", "crypto", "trading"],
  "entities": [
    {
      "type": "ORG",
      "text": "CompanyName"
    }
  ],
  "meta": {
    "source_url": "https://www.reddit.com/r/CryptoCurrency/comments/...",
    "subreddit": "CryptoCurrency",
    "score": 100,
    "upvote_ratio": 0.85,
    "num_comments": 25,
    "post_type": "text"
  },
  "processed_at": "2025-10-13T15:44:00Z",
  "processing_source": "pyflink"
}
```

### Indexes Created

The pipeline automatically creates these indexes for performance:

- `id` (unique): Primary identifier
- `platform`: Source platform
- `timestamp`: Post creation time (descending)
- `meta.subreddit`: Reddit subreddit
- `keywords`: Extracted keywords
- `processed_at`: Processing timestamp (descending)

## 🔍 Monitoring and Logging

### Logging Levels

- **INFO**: General pipeline progress and MongoDB operations
- **DEBUG**: Detailed processing information
- **WARNING**: Non-critical issues (e.g., duplicate records)
- **ERROR**: Processing errors and failures

### Key Metrics

The pipeline tracks and logs:

- Number of records processed
- MongoDB insertion/update counts
- Processing duration
- Error rates
- Duplicate detection

### Flink Web UI

Access the Flink dashboard at `http://localhost:8085` to monitor:

- Job status and metrics
- Task parallelism
- Checkpoints
- Throughput and latency
