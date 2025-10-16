#!/usr/bin/env python3
"""
Reddit Sentiment Analysis Consumer
Consumes Reddit data from Kafka, performs sentiment analysis using CryptoBERT,
and produces enriched data to silver layer
"""

import json
import logging
import re
from typing import Dict, Any, List, Optional
from datetime import datetime
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import sys
import os


class CryptoBERTSentimentConsumer:
    """Consumer that performs sentiment analysis on Reddit data using CryptoBERT"""
    
    def __init__(self, 
                 bootstrap_servers: str = 'localhost:9092',
                 input_topic: str = 'reddit-raw',
                 output_topic: str = 'reddit-silver'):
        """
        Initialize the sentiment analysis consumer
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            input_topic: Topic to consume raw Reddit data from
            output_topic: Topic to produce enriched data to
        """
        self.bootstrap_servers = bootstrap_servers
        self.input_topic = input_topic
        self.output_topic = output_topic
        
        # Initialize logging
        self.setup_logging()
        
        # Load model
        self.model_name = 'ElKulako/cryptobert'
        self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        self.logger.info(f"Using device: {self.device}")
        
        # Initialize model and tokenizer
        self.load_model()
        
        # Initialize Kafka connections
        self.init_kafka()
        
    def setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
    def load_model(self):
        """Load CryptoBERT model and tokenizer"""
        try:
            self.logger.info(f"Loading model: {self.model_name}")
            
            # Load tokenizer
            self.tokenizer = AutoTokenizer.from_pretrained(
                self.model_name,
                use_fast=True
            )
            
            # Load model
            self.model = AutoModelForSequenceClassification.from_pretrained(
                self.model_name
            ).to(self.device)
            
            # Labels: ['Bearish', 'Bullish', 'Neutral']
            self.label_map = {0: 'Bearish', 1: 'Bullish', 2: 'Neutral'}
            
            self.logger.info("Model loaded successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to load model: {e}")
            raise
    
    def init_kafka(self):
        """Initialize Kafka consumer and producer"""
        try:
            # Initialize consumer
            self.consumer = KafkaConsumer(
                self.input_topic,
                bootstrap_servers=self.bootstrap_servers,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
                group_id='reddit-sentiment-group',
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                max_poll_records=10,
                session_timeout_ms=30000
            )
            
            # Initialize producer
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3
            )
            
            self.logger.info("Kafka connections initialized")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Kafka: {e}")
            raise
    
    def preprocess_text(self, text: str) -> str:
        """
        Preprocess text for sentiment analysis
        
        Args:
            text: Input text
            
        Returns:
            Preprocessed text
        """
        if not text:
            return ""
        
        # Remove URLs
        text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text)
        
        # Remove mentions and hashtags
        text = re.sub(r'[@#]\w+', '', text)
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        
        # Remove non-alphanumeric chars except basic punctuation
        text = re.sub(r'[^\w\s.,!?;:]', '', text)
        
        return text
    
    def predict_sentiment(self, text: str, max_length: int = 512) -> Dict[str, Any]:
        """
        Predict sentiment using CryptoBERT
        
        Args:
            text: Input text
            max_length: Maximum sequence length
            
        Returns:
            Dictionary with sentiment prediction
        """
        try:
            # Preprocess text
            clean_text = self.preprocess_text(text)
            
            if not clean_text:
                return {
                    'sentiment': 'Neutral',
                    'sentiment_score': 0.0,
                    'confidence': 0.0,
                    'bearish_prob': 0.0,
                    'bullish_prob': 0.0,
                    'neutral_prob': 0.0
                }
            
            # Tokenize
            inputs = self.tokenizer(
                clean_text,
                truncation=True,
                padding=True,
                max_length=max_length,
                return_tensors='pt'
            ).to(self.device)
            
            # Predict
            with torch.no_grad():
                outputs = self.model(**inputs)
                probabilities = torch.nn.functional.softmax(outputs.logits, dim=-1)
                predicted_class = torch.argmax(probabilities, dim=-1).item()
                confidence = torch.max(probabilities).item()
            
            # Get probabilities for each class
            probs = probabilities[0].cpu().numpy()
            
            result = {
                'sentiment': self.label_map[predicted_class],
                'sentiment_score': float(predicted_class),
                'confidence': float(confidence),
                'bearish_prob': float(probs[0]),
                'bullish_prob': float(probs[1]),
                'neutral_prob': float(probs[2])
            }
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error predicting sentiment: {e}")
            return {
                'sentiment': 'Neutral',
                'sentiment_score': 0.0,
                'confidence': 0.0,
                'bearish_prob': 0.0,
                'bullish_prob': 0.0,
                'neutral_prob': 0.0
            }
    
    def enrich_data(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich Reddit data with sentiment analysis
        
        Args:
            message: Reddit message with metadata
            
        Returns:
            Enriched message with sentiment
        """
        try:
            data = message.get('data', {})
            metadata = message.get('metadata', {})
            
            # Extract text for analysis
            if metadata.get('type') == 'comment':
                text = data.get('text_clean', '')
            else:
                text = data.get('text_clean', '')
            
            # Perform sentiment analysis
            sentiment = self.predict_sentiment(text)
            
            # Create enriched data
            enriched_data = {
                'data': {
                    **data,  # Original data
                    'sentiment': sentiment,  # Add sentiment
                },
                'metadata': {
                    **metadata,
                    'processed_at': datetime.utcnow().isoformat(),
                    'layer': 'silver',  # Update layer to silver
                    'processing_version': '1.0'
                },
                'quality_metrics': {
                    'text_length': len(text) if text else 0,
                    'has_text': bool(text and len(text) > 10),
                    'sentiment_confidence': sentiment.get('confidence', 0.0)
                }
            }
            
            return enriched_data
            
        except Exception as e:
            self.logger.error(f"Error enriching data: {e}")
            return message
    
    def process_messages(self, timeout_ms: int = 1000):
        """
        Process messages from Kafka
        
        Args:
            timeout_ms: Timeout for polling
        """
        self.logger.info("Starting to process messages...")
        
        try:
            while True:
                # Poll for messages
                message_batch = self.consumer.poll(timeout_ms=timeout_ms)
                
                if not message_batch:
                    continue
                
                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        try:
                            # Process the message
                            enriched = self.enrich_data(message.value)
                            
                            # Send to output topic
                            self.producer.send(
                                topic=self.output_topic,
                                key=message.key,
                                value=enriched
                            )
                            
                            self.logger.debug(
                                f"Processed {message.value.get('metadata', {}).get('type', 'unknown')} "
                                f"with sentiment: {enriched['data']['sentiment']['sentiment']}"
                            )
                            
                        except Exception as e:
                            self.logger.error(f"Error processing message: {e}")
                            continue
                
                # Commit offsets
                self.consumer.commit_async()
                
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
            if self.producer:
                self.producer.flush()
                self.producer.close()
            self.logger.info("Cleanup completed")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")


def main():
    """Main function to run the sentiment analysis consumer"""
    consumer = CryptoBERTSentimentConsumer()
    consumer.process_messages()


if __name__ == "__main__":
    main()