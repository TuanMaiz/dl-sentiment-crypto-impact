#!/usr/bin/env python3
"""
Kafka Producer for Reddit Data
Sends scraped Reddit posts and comments to Kafka for processing
"""

import json
import logging
from typing import Dict, Any, List
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
import sys
import os


class RedditKafkaProducer:
    """Producer for sending Reddit data to Kafka"""
    
    def __init__(self, bootstrap_servers: str = 'localhost:9092', 
                 topic: str = 'reddit-raw'):
        """
        Initialize Kafka producer
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            topic: Topic name for Reddit data
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = None
        self.setup_logging()
        self.connect()
        
    def setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
    def connect(self):
        """Connect to Kafka and create producer"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',  # Wait for all replicas to ack
                retries=3,
                batch_size=16384,
                linger_ms=10,
                buffer_memory=33554432
            )
            self.logger.info(f"Connected to Kafka at {self.bootstrap_servers}")
            self.logger.info(f"Producing to topic: {self.topic}")
        except Exception as e:
            self.logger.error(f"Failed to connect to Kafka: {e}")
            raise
    
    def send_post(self, post_data: Dict[str, Any], 
                  post_type: str = 'post') -> bool:
        """
        Send a Reddit post to Kafka
        
        Args:
            post_data: Reddit post data
            post_type: Type of data ('post' or 'comment')
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Add metadata
            enriched_data = {
                'data': post_data,
                'metadata': {
                    'source': 'reddit',
                    'type': post_type,
                    'ingested_at': datetime.utcnow().isoformat(),
                    'layer': 'bronze'  # Medallion layer
                }
            }
            
            # Use post ID as key for partitioning
            key = post_data.get('id', None)
            
            # Send to Kafka
            future = self.producer.send(
                topic=self.topic,
                key=key,
                value=enriched_data
            )
            
            # Block for confirmation
            record_metadata = future.get(timeout=10)
            
            self.logger.debug(
                f"Sent {post_type} {key} to partition {record_metadata.partition} "
                f"at offset {record_metadata.offset}"
            )
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to send {post_type}: {e}")
            return False
    
    def send_batch(self, posts: List[Dict[str, Any]], 
                   post_type: str = 'post') -> int:
        """
        Send multiple posts to Kafka
        
        Args:
            posts: List of Reddit posts
            post_type: Type of data ('post' or 'comment')
            
        Returns:
            Number of successful sends
        """
        successful = 0
        
        self.logger.info(f"Sending batch of {len(posts)} {post_type}s...")
        
        for post in posts:
            if self.send_post(post, post_type):
                successful += 1
        
        self.logger.info(f"Successfully sent {successful}/{len(posts)} {post_type}s")
        return successful
    
    def send_post_with_comments(self, post_data: Dict[str, Any]) -> bool:
        """
        Send a post and all its comments to Kafka
        
        Args:
            post_data: Reddit post with comments
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Extract comments
            comments = post_data.pop('comments', [])
            comment_count = post_data.pop('comment_count', len(comments))
            
            # Send the main post
            if not self.send_post(post_data, 'post'):
                return False
            
            # Send comments if any
            if comments:
                successful_comments = self.send_batch(comments, 'comment')
                self.logger.info(f"Sent {successful_comments}/{comment_count} comments")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to send post with comments: {e}")
            return False
    
    def flush(self):
        """Flush any pending messages"""
        if self.producer:
            self.producer.flush()
            self.logger.info("Flushed all pending messages")
    
    def close(self):
        """Close the producer connection"""
        try:
            if self.producer:
                self.producer.flush()
                self.producer.close()
                self.logger.info("Closed Kafka producer")
        except Exception as e:
            self.logger.error(f"Error closing producer: {e}")


def main():
    """Example usage"""
    producer = RedditKafkaProducer()
    
    # Example post
    example_post = {
        "id": "reddit_test123",
        "platform": "reddit",
        "text_clean": "Bitcoin is going to the moon! 🚀",
        "post_created_timestamp": "2025-10-16T10:00:00Z",
        "meta": {
            "subreddit": "CryptoCurrency",
            "score": 100,
            "num_comments": 25
        }
    }
    
    # Send example
    success = producer.send_post(example_post, 'post')
    print(f"Success: {success}")
    
    producer.close()


if __name__ == "__main__":
    main()