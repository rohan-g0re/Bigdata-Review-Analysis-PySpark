import json
import time
from typing import Dict, List, Any, Callable
from src.streaming.utils.logging_config import setup_logger
from src.streaming.config.settings import USE_MOCK_KAFKA

logger = setup_logger("kafka_utils", "kafka_utils.log")

# Choose between real and mock Kafka based on settings
if USE_MOCK_KAFKA:
    logger.info("Using Mock Kafka implementation")
    from src.streaming.utils.mock_kafka import (
        create_mock_producer as create_producer,
        create_mock_consumer as create_consumer,
        create_mock_topic as create_kafka_topic
    )
else:
    logger.info("Using Real Kafka implementation")
    from kafka import KafkaProducer, KafkaConsumer
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError

    def create_producer(bootstrap_servers: str) -> KafkaProducer:
        """
        Create a Kafka producer with JSON serialization
        
        Args:
            bootstrap_servers (str): Comma-separated list of broker addresses
            
        Returns:
            KafkaProducer: Configured Kafka producer
        """
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                # Additional configurations for reliability
                acks='all',                   # Wait for all replicas
                retries=3,                    # Retry failed requests
                retry_backoff_ms=500,         # Backoff time between retries
                max_in_flight_requests_per_connection=1,  # For order preservation
                linger_ms=100,                # Batch messages
                batch_size=16384              # Batch size in bytes
            )
            logger.info(f"Created Kafka producer with bootstrap servers: {bootstrap_servers}")
            return producer
        except Exception as e:
            logger.error(f"Failed to create Kafka producer: {str(e)}")
            raise

    def create_consumer(bootstrap_servers: str, topic: str, group_id: str, 
                        auto_offset_reset: str = 'earliest') -> KafkaConsumer:
        """
        Create a Kafka consumer with JSON deserialization
        
        Args:
            bootstrap_servers (str): Comma-separated list of broker addresses
            topic (str): Topic to subscribe to
            group_id (str): Consumer group ID
            auto_offset_reset (str): Where to start reading messages ('earliest' or 'latest')
            
        Returns:
            KafkaConsumer: Configured Kafka consumer
        """
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_servers,
                group_id=group_id,
                auto_offset_reset=auto_offset_reset,
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                # Additional configurations
                enable_auto_commit=True,
                auto_commit_interval_ms=5000,  # Commit offsets every 5 seconds
                session_timeout_ms=30000       # Timeout for consumer heartbeats
            )
            logger.info(f"Created Kafka consumer for topic {topic} with group ID {group_id}")
            return consumer
        except Exception as e:
            logger.error(f"Failed to create Kafka consumer: {str(e)}")
            raise

    def create_kafka_topic(bootstrap_servers, topic_name, num_partitions=3, replication_factor=1):
        """
        Create a Kafka topic with the specified parameters
        
        Args:
            bootstrap_servers (str): Kafka bootstrap servers
            topic_name (str): Name of the topic to create
            num_partitions (int): Number of partitions for the topic
            replication_factor (int): Replication factor for the topic
            
        Returns:
            bool: True if topic was created or already exists, False otherwise
        """
        admin_client = None
        try:
            # Create admin client
            admin_client = KafkaAdminClient(
                bootstrap_servers=bootstrap_servers,
                client_id='steam-review-admin'
            )
            
            # Create topic
            topic = NewTopic(
                name=topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor
            )
            
            admin_client.create_topics([topic])
            logger.info(f"Successfully created topic '{topic_name}' with {num_partitions} partitions")
            return True
            
        except TopicAlreadyExistsError:
            logger.info(f"Topic '{topic_name}' already exists")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create topic '{topic_name}': {str(e)}")
            return False
            
        finally:
            if admin_client:
                admin_client.close()

def send_messages(producer, topic: str, messages: List[Dict[str, Any]], 
                  throttle_rate: float = 0.0) -> None:
    """
    Send a list of messages to a Kafka topic with optional throttling
    
    Args:
        producer: Kafka producer (real or mock)
        topic (str): Topic to send messages to
        messages (List[Dict]): List of messages to send
        throttle_rate (float): Seconds to wait between messages (0 for no throttling)
    """
    for i, message in enumerate(messages):
        try:
            producer.send(topic, value=message)
            logger.debug(f"Sent message {i+1}/{len(messages)} to topic {topic}")
            
            # Throttle if specified
            if throttle_rate > 0:
                time.sleep(throttle_rate)
                
            # Periodically flush to avoid keeping too many messages in memory
            if (i + 1) % 100 == 0:
                producer.flush()
                logger.debug(f"Flushed producer after {i+1} messages")
                
        except Exception as e:
            logger.error(f"Failed to send message {i+1}: {str(e)}")
    
    # Final flush to ensure all messages are sent
    producer.flush()
    logger.info(f"Successfully sent {len(messages)} messages to topic {topic}")

def consume_messages(consumer, 
                     handler: Callable[[Dict[str, Any]], None],
                     max_messages: int = None,
                     timeout_ms: int = 1000) -> int:
    """
    Consume messages from a Kafka topic with a custom handler
    
    Args:
        consumer: Kafka consumer (real or mock)
        handler (Callable): Function to handle each message
        max_messages (int, optional): Maximum number of messages to consume. If None, runs indefinitely.
        timeout_ms (int): Timeout in milliseconds for polling
        
    Returns:
        int: Number of messages consumed
    """
    count = 0
    try:
        while max_messages is None or count < max_messages:
            # Poll for messages with timeout
            messages = consumer.poll(timeout_ms=timeout_ms)
            
            if not messages:
                continue
                
            # Process messages
            for topic_partition, partition_messages in messages.items():
                for message in partition_messages:
                    handler(message.value)
                    count += 1
                    
                    if max_messages is not None and count >= max_messages:
                        logger.info(f"Reached maximum messages to consume: {max_messages}")
                        return count
                        
        return count
    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user")
        return count
    except Exception as e:
        logger.error(f"Error consuming messages: {str(e)}")
        return count

def create_producer(bootstrap_servers: str):
    """Create a Kafka producer"""
    if USE_MOCK_KAFKA:
        logger.info("Using Mock Kafka implementation")
        from src.streaming.utils.mock_kafka import MockKafkaProducer
        return MockKafkaProducer()
    else:
        logger.info(f"Creating Kafka producer with bootstrap servers: {bootstrap_servers}")
        from kafka import KafkaProducer
        return KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            api_version=(1, 0, 0)
        )

def get_kafka_producer(bootstrap_servers: str):
    """
    Get a Kafka producer (alias for create_producer)
    
    Args:
        bootstrap_servers: Kafka bootstrap servers
        
    Returns:
        A Kafka producer
    """
    return create_producer(bootstrap_servers) 