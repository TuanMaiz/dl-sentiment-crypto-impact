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

```bash
# From project root directory
docker compose up -d

# Submit job to Flink cluster
cd main
docker exec -it <jobmanager_container> python -m flink_processing.mongodb_ingestion_operator
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
