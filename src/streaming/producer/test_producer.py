"""
Test script for the Kafka producer.
This verifies that the producer can successfully stream data to Kafka.
"""
import argparse
import time
import json
from typing import Dict, Any

from src.streaming.utils.logging_config import setup_logger
from src.streaming.producer.kafka_producer import create_and_start_producer
from src.streaming.utils.kafka_utils import create_consumer
from src.streaming.config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    KAFKA_CONSUMER_GROUP,
    PARQUET_DIR,
    BATCH_SIZE,
    THROTTLE_RATE,
    USE_MOCK_KAFKA
)

logger = setup_logger("test_producer", "test_producer.log")

def test_producer_sample(
    bootstrap_servers: str,
    topic: str,
    parquet_dir: str,
    batch_size: int,
    throttle_rate: float,
    sample_size: int,
    show_sample: bool
) -> bool:
    """
    Test the producer by sending a sample of records
    
    Args:
        bootstrap_servers (str): Kafka bootstrap servers
        topic (str): Kafka topic
        parquet_dir (str): Directory containing Parquet files
        batch_size (int): Batch size for reading Parquet files
        throttle_rate (float): Seconds to wait between batches
        sample_size (int): Number of records to stream in sample mode
        show_sample (bool): Whether to print sample data
        
    Returns:
        bool: True if the test was successful
    """
    try:
        logger.info("Testing producer with sample data")
        
        # Create and start producer in sample mode
        producer_result = create_and_start_producer(
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            parquet_dir=parquet_dir,
            batch_size=batch_size,
            throttle_rate=throttle_rate,
            sample_mode=True,
            sample_size=sample_size,
            show_sample=show_sample
        )
        
        if not producer_result:
            logger.error("Producer failed")
            return False
        
        logger.info("Producer completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Sample producer test failed: {str(e)}")
        return False

def test_producer_and_consumer(
    bootstrap_servers: str,
    topic: str,
    group_id: str,
    parquet_dir: str,
    batch_size: int,
    throttle_rate: float,
    sample_size: int,
    consumer_timeout: int
) -> bool:
    """
    Test the full producer and consumer flow
    
    Args:
        bootstrap_servers (str): Kafka bootstrap servers
        topic (str): Kafka topic
        group_id (str): Consumer group ID
        parquet_dir (str): Directory containing Parquet files
        batch_size (int): Batch size for reading Parquet files
        throttle_rate (float): Seconds to wait between batches
        sample_size (int): Number of records to stream
        consumer_timeout (int): How long to wait for consumer (seconds)
        
    Returns:
        bool: True if the test was successful
    """
    try:
        logger.info("Testing producer and consumer flow")
        
        # First, create and start the producer in a separate thread
        import threading
        
        producer_result = [False]  # Use list to store result from thread
        
        def producer_thread():
            try:
                result = create_and_start_producer(
                    bootstrap_servers=bootstrap_servers,
                    topic=topic,
                    parquet_dir=parquet_dir,
                    batch_size=batch_size,
                    throttle_rate=throttle_rate,
                    max_records=sample_size,
                    sample_mode=False
                )
                producer_result[0] = result
            except Exception as e:
                logger.error(f"Producer thread error: {str(e)}")
        
        # Start the producer thread
        thread = threading.Thread(target=producer_thread)
        thread.start()
        
        # Give the producer a moment to start
        time.sleep(2)
        
        # Create a consumer
        logger.info(f"Creating consumer for topic {topic}")
        consumer = create_consumer(bootstrap_servers, topic, group_id)
        
        # Set up data collection
        messages_received = []
        
        def message_handler(message):
            messages_received.append(message)
            logger.info(f"Received message: {json.dumps(message)[:100]}...")
        
        # Start consuming with timeout
        logger.info(f"Starting consumer for {consumer_timeout} seconds")
        start_time = time.time()
        
        while time.time() - start_time < consumer_timeout and len(messages_received) < sample_size:
            # Poll for messages
            records = consumer.poll(timeout_ms=1000)
            
            if not records:
                logger.debug("No messages received in this poll")
                continue
                
            # Process records
            for tp, messages in records.items():
                for message in messages:
                    message_handler(message.value)
        
        # Close consumer
        consumer.close()
        
        # Wait for producer thread to finish
        thread.join(timeout=10)
        
        # Report results
        logger.info(f"Consumer received {len(messages_received)} messages")
        logger.info(f"Producer result: {'Success' if producer_result[0] else 'Failed'}")
        
        success = producer_result[0] and len(messages_received) > 0
        if success:
            logger.info("Producer/consumer test completed successfully")
        else:
            logger.error("Producer/consumer test failed")
            
        return success
        
    except Exception as e:
        logger.error(f"Producer/consumer test failed: {str(e)}")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Test the Kafka producer')
    parser.add_argument('--bootstrap-servers', default=KAFKA_BOOTSTRAP_SERVERS,
                        help='Kafka bootstrap servers')
    parser.add_argument('--topic', default=KAFKA_TOPIC,
                        help='Kafka topic')
    parser.add_argument('--group-id', default=KAFKA_CONSUMER_GROUP,
                        help='Consumer group ID')
    parser.add_argument('--parquet-dir', default=PARQUET_DIR,
                        help='Directory containing Parquet files')
    parser.add_argument('--batch-size', type=int, default=BATCH_SIZE,
                        help='Batch size for reading Parquet files')
    parser.add_argument('--throttle-rate', type=float, default=THROTTLE_RATE,
                        help='Seconds to wait between batches')
    parser.add_argument('--test', choices=['sample', 'full', 'both'], default='both',
                        help='Which test to run (sample, full, or both)')
    parser.add_argument('--sample-size', type=int, default=10,
                        help='Number of records to stream in sample mode')
    parser.add_argument('--show-sample', action='store_true',
                        help='Print sample data when in sample mode')
    parser.add_argument('--consumer-timeout', type=int, default=30,
                        help='How long to wait for consumer (seconds)')
    
    args = parser.parse_args()
    
    logger.info(f"Using {'Mock' if USE_MOCK_KAFKA else 'Real'} Kafka implementation")
    
    if args.test in ['sample', 'both']:
        print("\n----- Testing Producer Sample -----")
        success = test_producer_sample(
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
            parquet_dir=args.parquet_dir,
            batch_size=args.batch_size,
            throttle_rate=args.throttle_rate,
            sample_size=args.sample_size,
            show_sample=args.show_sample
        )
        print("Producer sample test:", "SUCCESS" if success else "FAILED")
    
    if args.test in ['full', 'both']:
        print("\n----- Testing Producer and Consumer Flow -----")
        success = test_producer_and_consumer(
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
            group_id=args.group_id,
            parquet_dir=args.parquet_dir,
            batch_size=args.batch_size,
            throttle_rate=args.throttle_rate,
            sample_size=args.sample_size,
            consumer_timeout=args.consumer_timeout
        )
        print("Producer/consumer test:", "SUCCESS" if success else "FAILED") 