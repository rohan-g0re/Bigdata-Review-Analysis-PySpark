"""
Mock Kafka implementation for testing without a real Kafka broker.
This uses a simple in-memory message queue to simulate Kafka.
"""
import json
import time
import threading
import queue
from typing import Dict, List, Any, Callable
from src.streaming.utils.logging_config import setup_logger

logger = setup_logger("mock_kafka", "mock_kafka.log")

# Global dictionary to store topics and their message queues
_topics = {}
_topic_lock = threading.Lock()

class MockKafkaProducer:
    """Mock implementation of KafkaProducer"""
    
    def __init__(self, bootstrap_servers, value_serializer=None, **kwargs):
        """Initialize the mock producer"""
        self.bootstrap_servers = bootstrap_servers
        self.value_serializer = value_serializer or (lambda v: json.dumps(v).encode('utf-8'))
        self.closed = False
        logger.info(f"Created Mock Kafka producer with bootstrap servers: {bootstrap_servers}")
    
    def send(self, topic, value, **kwargs):
        """Send a message to a topic"""
        if self.closed:
            raise RuntimeError("Producer is closed")
            
        with _topic_lock:
            if topic not in _topics:
                _topics[topic] = queue.Queue()
                logger.info(f"Created new topic: {topic}")
        
        # Serialize the value
        serialized_value = self.value_serializer(value)
        
        # Put the message in the queue
        _topics[topic].put(serialized_value)
        logger.debug(f"Sent message to topic {topic}")
        
        # Create a mock future
        class MockFuture:
            def get(self, timeout=None):
                return {'topic': topic, 'partition': 0, 'offset': 0}
        
        return MockFuture()
    
    def flush(self):
        """Flush all messages"""
        logger.debug("Flushed producer")
    
    def close(self):
        """Close the producer"""
        self.closed = True
        logger.info("Closed producer")

class MockKafkaConsumer:
    """Mock implementation of KafkaConsumer"""
    
    def __init__(self, *topics, bootstrap_servers=None, group_id=None, 
                 auto_offset_reset='earliest', value_deserializer=None, **kwargs):
        """Initialize the mock consumer"""
        self.topics = list(topics)
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.auto_offset_reset = auto_offset_reset
        self.value_deserializer = value_deserializer or (lambda v: json.loads(v.decode('utf-8')))
        self.closed = False
        
        # Create topics if they don't exist
        with _topic_lock:
            for topic in self.topics:
                if topic not in _topics:
                    _topics[topic] = queue.Queue()
                    logger.info(f"Created new topic: {topic}")
        
        logger.info(f"Created Mock Kafka consumer for topics {self.topics} with group ID {group_id}")
    
    def poll(self, timeout_ms=None, max_records=None):
        """Poll for new messages"""
        if self.closed:
            raise RuntimeError("Consumer is closed")
            
        timeout_sec = timeout_ms / 1000.0 if timeout_ms is not None else None
        start_time = time.time()
        result = {}
        
        # Try to get messages for each topic
        for topic in self.topics:
            if topic in _topics:
                partition_messages = []
                
                # Keep trying until we get a message or timeout
                while True:
                    try:
                        # Check for timeout
                        if timeout_sec is not None and time.time() - start_time >= timeout_sec:
                            break
                            
                        # Try to get a message with a small timeout
                        serialized_msg = _topics[topic].get(block=True, timeout=0.1)
                        
                        # Create a mock message
                        class MockMessage:
                            def __init__(self, value):
                                self.topic = topic
                                self.partition = 0
                                self.offset = 0
                                self.timestamp = int(time.time() * 1000)
                                self._serialized_value = value
                                self._value = None
                            
                            @property
                            def value(self):
                                if self._value is None:
                                    # Apply the deserializer if provided
                                    self._value = self.value_deserializer(self._serialized_value) 
                                return self._value
                        
                        # Create message and add a reference to the deserializer
                        msg = MockMessage(serialized_msg)
                        msg.value_deserializer = self.value_deserializer
                        
                        # Add the message to the result
                        partition_messages.append(msg)
                        
                        # Check if we have enough messages
                        if max_records is not None and len(partition_messages) >= max_records:
                            break
                    except queue.Empty:
                        # No more messages for now
                        break
                
                if partition_messages:
                    # Add the messages to the result
                    tp = (topic, 0)  # (topic, partition)
                    result[tp] = partition_messages
        
        return result
    
    def close(self):
        """Close the consumer"""
        self.closed = True
        logger.info("Closed consumer")

def create_mock_producer(bootstrap_servers: str, **kwargs) -> MockKafkaProducer:
    """
    Create a mock Kafka producer
    
    Args:
        bootstrap_servers (str): Comma-separated list of broker addresses
        
    Returns:
        MockKafkaProducer: Configured mock Kafka producer
    """
    try:
        producer = MockKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            **kwargs
        )
        return producer
    except Exception as e:
        logger.error(f"Failed to create mock Kafka producer: {str(e)}")
        raise

def create_mock_consumer(bootstrap_servers: str, topic: str, group_id: str, 
                    auto_offset_reset: str = 'earliest', **kwargs) -> MockKafkaConsumer:
    """
    Create a mock Kafka consumer
    
    Args:
        bootstrap_servers (str): Comma-separated list of broker addresses
        topic (str): Topic to subscribe to
        group_id (str): Consumer group ID
        auto_offset_reset (str): Where to start reading messages ('earliest' or 'latest')
        
    Returns:
        MockKafkaConsumer: Configured mock Kafka consumer
    """
    try:
        consumer = MockKafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            **kwargs
        )
        return consumer
    except Exception as e:
        logger.error(f"Failed to create mock Kafka consumer: {str(e)}")
        raise

# Function to create a Kafka topic (in this mock implementation, topics are created automatically)
def create_mock_topic(bootstrap_servers, topic_name, num_partitions=1, replication_factor=1):
    """
    Create a mock Kafka topic
    
    Args:
        bootstrap_servers (str): Kafka bootstrap servers
        topic_name (str): Name of the topic to create
        num_partitions (int): Number of partitions for the topic
        replication_factor (int): Replication factor for the topic
        
    Returns:
        bool: Always returns True as topics are created automatically
    """
    with _topic_lock:
        if topic_name not in _topics:
            _topics[topic_name] = queue.Queue()
            logger.info(f"Created new topic: {topic_name}")
    return True 