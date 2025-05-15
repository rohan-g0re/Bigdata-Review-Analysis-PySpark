import json
import time
import argparse
from src.streaming.utils.logging_config import setup_logger
from src.streaming.utils.kafka_utils import create_producer, create_consumer, send_messages
from src.streaming.config.settings import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, KAFKA_CONSUMER_GROUP, USE_MOCK_KAFKA

logger = setup_logger("test_kafka", "test_kafka.log")

def producer_test(bootstrap_servers, topic, num_messages=10):
    """
    Test Kafka producer by sending test messages
    
    Args:
        bootstrap_servers (str): Kafka bootstrap servers
        topic (str): Topic to send messages to
        num_messages (int): Number of test messages to send
    """
    try:
        # Create producer
        producer = create_producer(bootstrap_servers)
        
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
        send_messages(producer, topic, messages, throttle_rate=0.1)
        
        producer.close()
        logger.info("Producer test completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Producer test failed: {str(e)}")
        return False

def consumer_test(bootstrap_servers, topic, group_id, timeout_sec=30):
    """
    Test Kafka consumer by consuming messages for a set duration
    
    Args:
        bootstrap_servers (str): Kafka bootstrap servers
        topic (str): Topic to consume from
        group_id (str): Consumer group ID
        timeout_sec (int): How many seconds to run the consumer
    """
    try:
        # Create consumer
        consumer = create_consumer(bootstrap_servers, topic, group_id)
        
        # Set up message handler
        messages_received = []
        
        def message_handler(message):
            messages_received.append(message)
            logger.info(f"Received message: {json.dumps(message)[:100]}...")
        
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
                    message_handler(message.value)
        
        # Close consumer
        consumer.close()
        
        # Report results
        logger.info(f"Consumer test completed. Received {len(messages_received)} messages")
        return len(messages_received) > 0
        
    except Exception as e:
        logger.error(f"Consumer test failed: {str(e)}")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Test Kafka producer and consumer')
    parser.add_argument('--bootstrap-servers', default=KAFKA_BOOTSTRAP_SERVERS,
                        help='Kafka bootstrap servers')
    parser.add_argument('--topic', default=KAFKA_TOPIC,
                        help='Topic name')
    parser.add_argument('--group-id', default=KAFKA_CONSUMER_GROUP,
                        help='Consumer group ID')
    parser.add_argument('--mode', choices=['producer', 'consumer', 'both'], default='both',
                        help='Test mode (producer, consumer, or both)')
    parser.add_argument('--num-messages', type=int, default=10,
                        help='Number of test messages to send (producer only)')
    parser.add_argument('--timeout', type=int, default=30,
                        help='Consumer timeout in seconds (consumer only)')
    
    args = parser.parse_args()
    
    logger.info(f"Using {'Mock' if USE_MOCK_KAFKA else 'Real'} Kafka implementation")
    
    if args.mode in ['producer', 'both']:
        producer_result = producer_test(args.bootstrap_servers, args.topic, args.num_messages)
        if args.mode == 'both':
            # Give Kafka a moment to process the messages
            time.sleep(2)
    
    if args.mode in ['consumer', 'both']:
        consumer_result = consumer_test(args.bootstrap_servers, args.topic, args.group_id, args.timeout)
    
    if args.mode == 'both':
        if producer_result and consumer_result:
            print("Kafka test completed successfully!")
            exit(0)
        else:
            print("Kafka test failed. Check logs for details.")
            exit(1)
    elif args.mode == 'producer':
        if producer_result:
            print("Producer test completed successfully!")
            exit(0)
        else:
            print("Producer test failed. Check logs for details.")
            exit(1)
    elif args.mode == 'consumer':
        if consumer_result:
            print("Consumer test completed successfully!")
            exit(0)
        else:
            print("Consumer test failed. Check logs for details.")
            exit(1) 