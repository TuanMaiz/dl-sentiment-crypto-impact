#!/usr/bin/env python3
"""
Silver Data Producer for MongoDB
Consumes enriched Reddit data from Kafka and stores it in MongoDB silver layer
following medallion architecture pattern
"""

import json
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from pymongo import MongoClient, errors as pymongo_errors
from pymongo.collection import Collection
from pymongo.database import Database
import sys
import os


class MongoDBSilverProducer:
    """Producer that stores enriched Reddit data to MongoDB silver layer"""
    
    def __init__(self,
                 bootstrap_servers: str = 'localhost:9092',
                 kafka_topic: str = 'reddit-silver',
                 mongo_uri: str = 'mongodb://localhost:27017',
                 mongo_db: str = 'reddit_silver'):
        """
        Initialize the MongoDB silver producer
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            kafka_topic: Topic to consume enriched data from
            mongo_uri: MongoDB connection URI
            mongo_db: MongoDB database name
        """
        self.bootstrap_servers = bootstrap_servers
        self.kafka_topic = kafka_topic
        self.mongo_uri = mongo_uri
        self.mongo_db_name = mongo_db
        
        # Initialize logging
        self.setup_logging()
        
        # Initialize MongoDB connection
        self.init_mongodb()
        
        # Initialize Kafka consumer
        self.init_kafka()
        
        # Initialize collections
        self.init_collections()
        
    def setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
    def init_mongodb(self):
        """Initialize MongoDB connection"""
        try:
            self.client = MongoClient(
                self.mongo_uri,
                serverSelectionTimeoutMS=5000
            )
            
            # Test connection
            self.client.admin.command('ping')
            self.logger.info("Connected to MongoDB")
            
            self.db: Database = self.client[self.mongo_db_name]
            
        except Exception as e:
            self.logger.error(f"Failed to connect to MongoDB: {e}")
            raise
    
    def init_kafka(self):
        """Initialize Kafka consumer"""
        try:
            self.consumer = KafkaConsumer(
                self.kafka_topic,
                bootstrap_servers=self.bootstrap_servers,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
                group_id='reddit-silver-consumer',
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                max_poll_records=50,
                session_timeout_ms=30000
            )
            
            self.logger.info(f"Connected to Kafka, consuming from topic: {self.kafka_topic}")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Kafka consumer: {e}")
            raise
    
    def init_collections(self):
        """Initialize MongoDB collections with proper indexes"""
        try:
            # Collections for different data types
            self.posts_collection: Collection = self.db['posts']
            self.comments_collection: Collection = self.db['comments']
            self.aggregated_collection: Collection = self.db['aggregated_sentiment']
            self.quality_metrics_collection: Collection = self.db['quality_metrics']
            
            # Create indexes for posts
            self.posts_collection.create_index(
                [('data.id', 1)],
                unique=True
            )
            self.posts_collection.create_index(
                [('data.post_created_timestamp', -1)]
            )
            self.posts_collection.create_index(
                [('data.meta.subreddit', 1)]
            )
            self.posts_collection.create_index(
                [('data.sentiment.sentiment', 1)]
            )
            
            # Create indexes for comments
            self.comments_collection.create_index(
                [('data.id', 1)],
                unique=True
            )
            self.comments_collection.create_index(
                [('data.comment_created_timestamp', -1)]
            )
            self.comments_collection.create_index(
                [('data.post_permalink', 1)]
            )
            self.comments_collection.create_index(
                [('data.sentiment.sentiment', 1)]
            )
            
            # Create indexes for aggregated sentiment
            self.aggregated_collection.create_index(
                [('date', -1), ('subreddit', 1), ('data_type', 1)],
                unique=True
            )
            
            # Create indexes for quality metrics
            self.quality_metrics_collection.create_index(
                [('date', -1)]
            )
            
            self.logger.info("MongoDB collections and indexes initialized")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize collections: {e}")
            raise
    
    def validate_silver_data(self, data: Dict[str, Any]) -> bool:
        """
        Validate that data meets silver layer quality requirements
        
        Args:
            data: Enriched data from sentiment analysis
            
        Returns:
            True if valid, False otherwise
        """
        try:
            # Check required fields
            required_fields = ['data', 'metadata', 'quality_metrics']
            for field in required_fields:
                if field not in data:
                    return False
            
            # Check sentiment data
            if 'sentiment' not in data['data']:
                return False
            
            # Check timestamp
            if not data['data'].get('post_created_timestamp') and not data['data'].get('comment_created_timestamp'):
                return False
            
            # Check minimum quality
            quality = data.get('quality_metrics', {})
            if quality.get('sentiment_confidence', 0) < 0.5:
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error validating data: {e}")
            return False
    
    def store_post(self, data: Dict[str, Any]) -> bool:
        """
        Store a post in the silver layer
        
        Args:
            data: Enriched post data
            
        Returns:
            True if successful, False otherwise
        """
        try:
            post_id = data['data']['id']
            
            # Update or insert post
            result = self.posts_collection.update_one(
                {'data.id': post_id},
                {
                    '$set': {
                        **data,
                        'metadata.updated_at': datetime.utcnow()
                    },
                    '$setOnInsert': {
                        'metadata.created_at': datetime.utcnow(),
                        'metadata.source_system': 'reddit-scraper'
                    }
                },
                upsert=True
            )
            
            if result.upserted_id:
                self.logger.debug(f"Inserted new post: {post_id}")
            else:
                self.logger.debug(f"Updated existing post: {post_id}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error storing post: {e}")
            return False
    
    def store_comment(self, data: Dict[str, Any]) -> bool:
        """
        Store a comment in the silver layer
        
        Args:
            data: Enriched comment data
            
        Returns:
            True if successful, False otherwise
        """
        try:
            comment_id = data['data']['id']
            
            # Update or insert comment
            result = self.comments_collection.update_one(
                {'data.id': comment_id},
                {
                    '$set': {
                        **data,
                        'metadata.updated_at': datetime.utcnow()
                    },
                    '$setOnInsert': {
                        'metadata.created_at': datetime.utcnow(),
                        'metadata.source_system': 'reddit-scraper'
                    }
                },
                upsert=True
            )
            
            if result.upserted_id:
                self.logger.debug(f"Inserted new comment: {comment_id}")
            else:
                self.logger.debug(f"Updated existing comment: {comment_id}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error storing comment: {e}")
            return False
    
    def update_aggregated_sentiment(self, data: Dict[str, Any]):
        """
        Update aggregated sentiment statistics
        
        Args:
            data: Enriched data
        """
        try:
            data_type = data['metadata']['type']
            sentiment = data['data']['sentiment']['sentiment']
            confidence = data['data']['sentiment']['confidence']
            
            # Extract date and subreddit
            if data_type == 'post':
                timestamp_str = data['data']['post_created_timestamp']
                subreddit = data['data']['meta'].get('subreddit', 'unknown')
            else:
                timestamp_str = data['data']['comment_created_timestamp']
                subreddit = 'unknown'  # Comments might not have subreddit
            
            # Parse timestamp
            timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            date = timestamp.date()
            
            # Update aggregation
            self.aggregated_collection.update_one(
                {
                    'date': date,
                    'subreddit': subreddit,
                    'data_type': data_type
                },
                {
                    '$inc': {
                        'total_count': 1,
                        'sentiment_counts.Bullish': 1 if sentiment == 'Bullish' else 0,
                        'sentiment_counts.Bearish': 1 if sentiment == 'Bearish' else 0,
                        'sentiment_counts.Neutral': 1 if sentiment == 'Neutral' else 0,
                        'avg_confidence': confidence,
                        'confidence_count': 1
                    },
                    '$set': {
                        'metadata.updated_at': datetime.utcnow()
                    },
                    '$setOnInsert': {
                        'metadata.created_at': datetime.utcnow()
                    }
                },
                upsert=True
            )
            
            # Update average confidence
            self.db.command({
                'update': self.aggregated_collection.name,
                'updates': [{
                    'find': {
                        'date': date,
                        'subreddit': subreddit,
                        'data_type': data_type
                    },
                    'update': {
                        '$set': {
                            'avg_confidence': {
                                '$divide': ['$avg_confidence', '$confidence_count']
                            }
                        }
                    }
                }]
            })
            
        except Exception as e:
            self.logger.error(f"Error updating aggregated sentiment: {e}")
    
    def process_messages(self, timeout_ms: int = 1000):
        """
        Process messages from Kafka and store to MongoDB
        
        Args:
            timeout_ms: Timeout for polling
        """
        self.logger.info("Starting to process messages...")
        
        batch_size = 0
        batch_start = datetime.utcnow()
        
        try:
            while True:
                # Poll for messages
                message_batch = self.consumer.poll(timeout_ms=timeout_ms)
                
                if not message_batch:
                    # Check if we need to commit
                    if batch_size > 0:
                        self.consumer.commit_async()
                        self.logger.debug(f"Committed batch of {batch_size} messages")
                        batch_size = 0
                    continue
                
                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        try:
                            # Validate data
                            if not self.validate_silver_data(message.value):
                                self.logger.warning("Skipping invalid data")
                                continue
                            
                            data_type = message.value['metadata']['type']
                            
                            # Store based on type
                            if data_type == 'post':
                                success = self.store_post(message.value)
                            elif data_type == 'comment':
                                success = self.store_comment(message.value)
                            else:
                                self.logger.warning(f"Unknown data type: {data_type}")
                                success = False
                            
                            # Update aggregation
                            if success:
                                self.update_aggregated_sentiment(message.value)
                                batch_size += 1
                            
                        except Exception as e:
                            self.logger.error(f"Error processing message: {e}")
                            continue
                
                # Commit after batch
                if batch_size >= 100:
                    self.consumer.commit_async()
                    self.logger.info(f"Committed batch of {batch_size} messages")
                    batch_size = 0
                
        except KeyboardInterrupt:
            self.logger.info("Stopping consumer...")
        except Exception as e:
            self.logger.error(f"Error in consumer loop: {e}")
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Cleanup resources"""
        try:
            if self.consumer:
                self.consumer.close()
            if self.client:
                self.client.close()
            self.logger.info("Cleanup completed")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")


def main():
    """Main function to run the MongoDB silver producer"""
    producer = MongoDBSilverProducer()
    producer.process_messages()


if __name__ == "__main__":
    main()