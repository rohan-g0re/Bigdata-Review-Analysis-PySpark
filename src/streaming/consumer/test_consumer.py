"""
Test script for Kafka consumer
"""
import os
import time
import json
import argparse
import pandas as pd
from src.streaming.producer.kafka_producer import create_and_start_producer
from src.streaming.consumer.kafka_consumer import create_and_start_consumer
from src.streaming.utils.kafka_utils import create_producer, create_consumer
from src.streaming.config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    KAFKA_CONSUMER_GROUP,
    STREAM_RESULTS_DIR,
    USE_MOCK_KAFKA
)
from src.streaming.utils.logging_config import setup_logger

# Set up logging
logger = setup_logger("test_consumer", "test_consumer.log")

def test_consumer_with_mock_data(sample_size=50, results_dir=None):
    """
    Test the consumer with mock data (no producer)
    
    Args:
        sample_size (int): Number of mock data points to generate
        results_dir (str): Directory to save results, if None uses a temp directory
    """
    # Use a temporary directory for test results if not specified
    if results_dir is None:
        results_dir = os.path.join(STREAM_RESULTS_DIR, "test_consumer_mock")
    
    # Ensure results directory exists
    os.makedirs(results_dir, exist_ok=True)
    
    # Generate sample data
    logger.info(f"Generating {sample_size} mock review records")
    
    # Create a producer to send mock data
    producer = create_producer(KAFKA_BOOTSTRAP_SERVERS)
    
    # Generate mock reviews
    mock_reviews = []
    for i in range(sample_size):
        game_id = i % 10 + 1  # 10 different games
        mock_review = {
            'app_id': game_id,
            'game': f"Game_{game_id}",  # Add game name based on ID
            'recommended': i % 3 != 0,  # 2/3 are positive
            'review': f"This is a mock review #{i+1} for testing the consumer.",
            'timestamp_created': pd.Timestamp.now().isoformat(),
            'votes_up': i % 5,
            'votes_funny': i % 3,
            'weighted_vote_score': 0.5 + (i % 10) / 20.0,
            'comment_count': i % 3,
            'steam_purchase': i % 2 == 0,
            'received_for_free': i % 5 == 0,
            'written_during_early_access': i % 4 == 0
        }
        mock_reviews.append(mock_review)
    
    # Start the consumer in a separate thread
    import threading
    
    consumer_thread = threading.Thread(
        target=create_and_start_consumer,
        kwargs={
            'bootstrap_servers': KAFKA_BOOTSTRAP_SERVERS,
            'topic': KAFKA_TOPIC,
            'group_id': KAFKA_CONSUMER_GROUP,
            'results_dir': results_dir,
            'max_messages': sample_size
        }
    )
    consumer_thread.daemon = True
    consumer_thread.start()
    
    # Wait a moment for consumer to initialize
    time.sleep(2)
    
    # Send the mock reviews to Kafka
    for review in mock_reviews:
        producer.send(KAFKA_TOPIC, value=review)
        time.sleep(0.05)  # Small delay between messages
    
    producer.flush()
    logger.info(f"Sent {len(mock_reviews)} mock reviews to Kafka")
    
    # Wait for consumer to process the messages
    consumer_thread.join(timeout=30)
    
    # Check if results files were created
    time.sleep(2)  # Give consumer time to save files
    files_created = os.listdir(results_dir)
    logger.info(f"Results directory contains: {files_created}")
    
    if 'top_games.csv' in files_created and 'sentiment_analysis.csv' in files_created:
        logger.info("Test successful: Consumer processed data and created output files")
        return True
    else:
        logger.error("Test failed: Consumer did not create expected output files")
        return False

def test_end_to_end(sample_size=50):
    """
    Test end-to-end producer to consumer flow
    
    Args:
        sample_size (int): Number of data points to process
    """
    # Set up test directories
    test_results_dir = os.path.join(STREAM_RESULTS_DIR, "test_e2e")
    os.makedirs(test_results_dir, exist_ok=True)
    
    # Start the consumer in a thread
    import threading
    
    logger.info("Starting consumer thread...")
    consumer_thread = threading.Thread(
        target=create_and_start_consumer,
        kwargs={
            'bootstrap_servers': KAFKA_BOOTSTRAP_SERVERS,
            'topic': KAFKA_TOPIC,
            'group_id': KAFKA_CONSUMER_GROUP,
            'results_dir': test_results_dir,
            'max_messages': sample_size
        }
    )
    consumer_thread.daemon = True
    consumer_thread.start()
    
    # Wait a bit for consumer to initialize
    time.sleep(2)
    
    # Start the producer (this will block until it completes)
    logger.info("Starting producer...")
    producer_success = create_and_start_producer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        topic=KAFKA_TOPIC,
        max_records=sample_size,
        sample_mode=True,
        sample_size=sample_size
    )
    
    if not producer_success:
        logger.error("Producer failed, aborting test")
        return False
    
    # Wait for consumer to process all messages
    logger.info("Waiting for consumer to process all messages...")
    consumer_thread.join(timeout=30)
    
    # Check if results files were created
    time.sleep(2)  # Give consumer time to save files
    files_created = os.listdir(test_results_dir)
    logger.info(f"Results directory contains: {files_created}")
    
    if 'top_games.csv' in files_created and 'sentiment_analysis.csv' in files_created:
        logger.info("End-to-end test successful: Data flowed from producer to consumer")
        
        # Print sample of results
        try:
            top_games = pd.read_csv(os.path.join(test_results_dir, 'top_games.csv'))
            logger.info(f"Top games: \n{top_games.head()}")
            
            sentiment = pd.read_csv(os.path.join(test_results_dir, 'sentiment_analysis.csv'))
            logger.info(f"Sentiment analysis: \n{sentiment.head()}")
        except Exception as e:
            logger.error(f"Error reading results: {e}")
        
        return True
    else:
        logger.error("End-to-end test failed: Consumer did not create expected output files")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test the Kafka consumer")
    parser.add_argument('--mode', choices=['mock', 'e2e'], default='mock',
                        help='Test mode: mock (consumer only) or e2e (end-to-end)')
    parser.add_argument('--samples', type=int, default=50,
                        help='Number of sample data points to use')
    args = parser.parse_args()
    
    logger.info(f"Starting consumer test in {args.mode} mode with {args.samples} samples")
    
    if args.mode == 'mock':
        success = test_consumer_with_mock_data(sample_size=args.samples)
    else:
        success = test_end_to_end(sample_size=args.samples)
    
    logger.info(f"Test completed: {'SUCCESS' if success else 'FAILURE'}") 