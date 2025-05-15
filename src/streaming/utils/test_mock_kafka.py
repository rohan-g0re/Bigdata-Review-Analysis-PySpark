"""
Test script for the mock Kafka implementation.
This verifies that our in-memory Kafka works correctly.
"""
import json
import time
import argparse
import sys
from src.streaming.utils.logging_config import setup_logger
from src.streaming.utils.mock_kafka import (
    create_mock_producer, 
    create_mock_consumer,
    create_mock_topic
)
from src.streaming.config.settings import KAFKA_TOPIC, KAFKA_CONSUMER_GROUP

logger = setup_logger("test_mock_kafka", "test_mock_kafka.log")

def producer_test(bootstrap_servers, topic, num_messages=10):
    """
    Test mock Kafka producer by sending test messages
    
    Args:
        bootstrap_servers (str): Kafka bootstrap servers
        topic (str): Topic to send messages to
        num_messages (int): Number of test messages to send
    """
    try:
        # Create topic (optional, will be created automatically if needed)
        create_mock_topic(bootstrap_servers, topic)
        
        # Create producer
        producer = create_mock_producer(bootstrap_servers)
        
        # Create test messages
        messages = [
            {
                "test_id": i,
                "message": f"Test message {i}",
                "timestamp": time.time()
            }
            for i in range(num_messages)
        ]
        
        # Send messages
        logger.info(f"Sending {num_messages} test messages to topic '{topic}'")
        for i, message in enumerate(messages):
            producer.send(topic, value=message)
            logger.debug(f"Sent message {i+1}/{len(messages)} to topic {topic}")
        
        producer.flush()
        logger.info(f"Sent {num_messages} messages to topic '{topic}'")
        
        producer.close()
        logger.info("Producer test completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Producer test failed: {str(e)}")
        return False

def consumer_test(bootstrap_servers, topic, group_id, timeout_sec=10):
    """
    Test mock Kafka consumer by consuming messages for a set duration
    
    Args:
        bootstrap_servers (str): Kafka bootstrap servers
        topic (str): Topic to consume from
        group_id (str): Consumer group ID
        timeout_sec (int): How many seconds to run the consumer
    """
    try:
        # Create consumer
        consumer = create_mock_consumer(bootstrap_servers, topic, group_id)
        
        # Set up message handler
        messages_received = []
        
        # Start consuming
        logger.info(f"Starting consumer test for {timeout_sec} seconds")
        start_time = time.time()
        
        while time.time() - start_time < timeout_sec:
            # Poll for messages
            records = consumer.poll(timeout_ms=1000)
            
            if not records:
                logger.debug("No messages received in this poll")
                continue
                
            # Process records
            for tp, messages in records.items():
                for message in messages:
                    value = message.value
                    value_dict = json.loads(value.decode('utf-8')) if isinstance(value, bytes) else value
                    messages_received.append(value_dict)
                    logger.info(f"Received message: {json.dumps(value_dict)[:100]}...")
        
        # Close consumer
        consumer.close()
        
        # Report results
        logger.info(f"Consumer test completed. Received {len(messages_received)} messages")
        return len(messages_received) > 0
        
    except Exception as e:
        logger.error(f"Consumer test failed: {str(e)}")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Test mock Kafka producer and consumer')
    parser.add_argument('--bootstrap-servers', default="localhost:9092",
                        help='Kafka bootstrap servers (unused in mock)')
    parser.add_argument('--topic', default=KAFKA_TOPIC,
                        help='Topic name')
    parser.add_argument('--group-id', default=KAFKA_CONSUMER_GROUP,
                        help='Consumer group ID')
    parser.add_argument('--mode', choices=['producer', 'consumer', 'both'], default='both',
                        help='Test mode (producer, consumer, or both)')
    parser.add_argument('--num-messages', type=int, default=10,
                        help='Number of test messages to send (producer only)')
    parser.add_argument('--timeout', type=int, default=10,
                        help='Consumer timeout in seconds (consumer only)')
    
    args = parser.parse_args()
    
    if args.mode in ['producer', 'both']:
        producer_result = producer_test(args.bootstrap_servers, args.topic, args.num_messages)
        if args.mode == 'both':
            # Give mock Kafka a moment
            time.sleep(1)
    
    if args.mode in ['consumer', 'both']:
        consumer_result = consumer_test(args.bootstrap_servers, args.topic, args.group_id, args.timeout)
    
    if args.mode == 'both':
        if producer_result and consumer_result:
            print("Mock Kafka test completed successfully!")
            sys.exit(0)
        else:
            print("Mock Kafka test failed. Check logs for details.")
            sys.exit(1)
    elif args.mode == 'producer':
        if producer_result:
            print("Producer test completed successfully!")
            sys.exit(0)
        else:
            print("Producer test failed. Check logs for details.")
            sys.exit(1)
    elif args.mode == 'consumer':
        if consumer_result:
            print("Consumer test completed successfully!")
            sys.exit(0)
        else:
            print("Consumer test failed. Check logs for details.")
            sys.exit(1) 