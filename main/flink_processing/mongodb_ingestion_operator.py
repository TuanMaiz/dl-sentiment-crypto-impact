#!/usr/bin/env python3
"""
PyFlink MongoDB Ingestion Script

This script reads Reddit post data from JSON files and ingests them into MongoDB
using PyFlink for distributed processing.
"""

import json
import os
import logging
from dotenv import load_dotenv
from typing import Dict, Any, List
from datetime import datetime

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.common import Configuration
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction, FlatMapFunction
from pyflink.common import Row

import pymongo
from pymongo import MongoClient


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()


class MongoDBConfig:
    """MongoDB configuration settings"""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 27017,
        database: str = "Bronze",
        collection: str = "reddit_post",
        username: str = None,
        password: str = None,
        connection_string: str = None,
    ):
        if connection_string:
            self.connection_string = connection_string
        else:
            self.host = host
            self.port = port
            self.database = database
            self.collection = collection
            self.username = username
            self.password = password

    def get_connection_string(self) -> str:
        """Generate MongoDB connection string"""
        # if self.username and self.password:
        #     return f"mongodb+srv://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        # else:
        #     return f"mongodb+srv://{self.host}:{self.port}/{self.database}"
        return self.connection_string


class JSONFileReader(FlatMapFunction):
    """Read and parse JSON files containing Reddit posts"""

    def __init__(self, file_path: str):
        self.file_path = file_path

    def flat_map(self, value):
        """Read JSON file and yield individual posts"""
        try:
            logger.info(f"Reading JSON file: {self.file_path}")

            if not os.path.exists(self.file_path):
                logger.error(f"File not found: {self.file_path}")
                return

            with open(self.file_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            # Handle both single post and array of posts
            if isinstance(data, list):
                posts = data
            else:
                posts = [data]

            for post in posts:
                # Validate post structure
                if self._validate_post(post):
                    yield Row(**post)
                else:
                    logger.warning(
                        f"Invalid post structure: {post.get('id', 'unknown')}"
                    )

        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e}")
        except Exception as e:
            logger.error(f"Error reading file {self.file_path}: {e}")

    def _validate_post(self, post: Dict[str, Any]) -> bool:
        """Validate post has required fields"""
        required_fields = ["id", "platform", "text_clean", "timestamp"]
        return all(field in post for field in required_fields)


class MongoDBSink(MapFunction):
    """MongoDB sink for writing processed posts"""

    def __init__(self, config: MongoDBConfig):
        self.config = config
        self.client = None
        self.db = None
        self.collection = None

    def open(self, runtime_context):
        """Initialize MongoDB connection"""
        try:
            self.client = MongoClient(self.config.get_connection_string())
            self.db = self.client[self.config.database]
            self.collection = self.db[self.config.collection]
            logger.info(
                f"Connected to MongoDB: {self.config.database}.{self.config.collection}"
            )

            # Create indexes for better query performance
            self._create_indexes()

        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise

    def map(self, post_row: Row):
        """Insert post into MongoDB"""
        try:
            # Convert Row to dictionary
            post_dict = (
                post_row.as_dict() if hasattr(post_row, "as_dict") else dict(post_row)
            )

            # Add processing metadata
            post_dict["processed_at"] = datetime.utcnow().isoformat()
            post_dict["processing_source"] = "pyflink"

            # Use upsert to avoid duplicates based on post ID
            query = {"id": post_dict["id"]}
            update = {"$set": post_dict}

            result = self.collection.update_one(query, update, upsert=True)

            if result.upserted_id:
                logger.info(f"Inserted new post: {post_dict['id']}")
            elif result.modified_count > 0:
                logger.info(f"Updated existing post: {post_dict['id']}")
            else:
                logger.debug(f"No changes for post: {post_dict['id']}")

            return post_dict

        except Exception as e:
            logger.error(f"Error inserting post to MongoDB: {e}")
            raise

    def close(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")

    def _create_indexes(self):
        """Create MongoDB indexes for performance"""
        try:
            indexes = [
                [("id", 1)],  # Unique index on post ID
                [("platform", 1)],  # Index on platform
                [("timestamp", -1)],  # Index on timestamp (descending)
                [("meta.subreddit", 1)],  # Index on subreddit
                [("keywords", 1)],  # Index on keywords
                [("processed_at", -1)],  # Index on processing time
            ]

            for index_spec in indexes:
                try:
                    self.collection.create_index(index_spec)
                    logger.info(f"Created index: {index_spec}")
                except pymongo.errors.DuplicateKeyError:
                    logger.debug(f"Index already exists: {index_spec}")

        except Exception as e:
            logger.warning(f"Error creating indexes: {e}")


class RedditToMongoDBPipeline:
    """Main pipeline for ingesting Reddit data into MongoDB"""

    def __init__(
        self, input_file_path: str, mongodb_config: MongoDBConfig, parallelism: int = 1
    ):
        self.input_file_path = input_file_path
        self.mongodb_config = mongodb_config
        self.parallelism = parallelism

    def run(self):
        """Execute the PyFlink pipeline"""
        logger.info("Starting Reddit to MongoDB ingestion pipeline")

        # Set up PyFlink environment
        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_parallelism(self.parallelism)

        # Enable checkpointing for fault tolerance
        env.enable_checkpointing(10000)  # 10 seconds

        # Create source from file path
        source = env.from_collection([self.input_file_path])

        # Read and parse JSON files
        posts_stream = source.flat_map(
            JSONFileReader(self.input_file_path),
            output_type=Types.ROW_NAMED(
                [
                    "id",
                    "platform",
                    "author_id",
                    "text_clean",
                    "lang",
                    "timestamp",
                    "keywords",
                    "entities",
                    "meta",
                ],
                [
                    Types.STRING(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.OBJECT_ARRAY(Types.STRING()),
                    Types.OBJECT_ARRAY(Types.GENERIC(dict)),
                    Types.GENERIC(dict),
                ],
            ),
        )

        # Process and sink to MongoDB
        result_stream = posts_stream.map(
            MongoDBSink(self.mongodb_config), output_type=Types.GENERIC(dict)
        )

        # Print results for monitoring
        result_stream.print("MongoDB_Ingestion")

        # Execute the pipeline
        try:
            job_result = env.execute("Reddit Posts to MongoDB Ingestion")
            logger.info(f"Job completed successfully: {job_result}")
            return job_result
        except Exception as e:
            logger.error(f"Job execution failed: {e}")
            raise


def create_default_config() -> MongoDBConfig:
    """Create default MongoDB configuration from environment variables"""
    return MongoDBConfig(
        database=os.getenv("MONGODB_DATABASE"),
        collection=os.getenv("MONGODB_COLLECTION"),
        connection_string=os.getenv("MONGODB_URL_CONNECTION_STRING"),
    )


def main():
    """Main entry point"""
    # Configuration
    input_file = "./data_scraping/output/reddit_posts_20251007_225355.json"
    mongodb_config = create_default_config()

    # Verify input file exists
    full_input_path = os.path.abspath(input_file)
    if not os.path.exists(full_input_path):
        logger.error(f"Input file not found: {full_input_path}")
        return 1

    logger.info(f"Input file: {full_input_path}")
    logger.info(f"MongoDB config: {mongodb_config.connection_string}")

    # Create and run pipeline
    try:
        pipeline = RedditToMongoDBPipeline(
            input_file_path=full_input_path,
            mongodb_config=mongodb_config,
            parallelism=2,
        )

        result = pipeline.run()
        logger.info("Pipeline completed successfully")
        return 0

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        return 1


if __name__ == "__main__":
    import sys

    sys.exit(main())
