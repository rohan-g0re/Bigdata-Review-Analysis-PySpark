"""
Kafka Producer for sending Steam review data from Parquet files to Kafka
"""
import time
import argparse
import json
import signal
import os
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime

from src.streaming.utils.logging_config import setup_logger
from src.streaming.utils.kafka_utils import create_producer, create_kafka_topic, send_messages, get_kafka_producer
from src.streaming.producer.parquet_reader import ParquetReader
from src.streaming.config.settings import (
    PARQUET_DIR,
    BATCH_SIZE,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    THROTTLE_RATE,
    USE_MOCK_KAFKA
)

# Add import for metrics collection
from src.streaming.monitoring.metrics_collector import producer_metrics, record_system_error

logger = setup_logger("kafka_producer", "kafka_producer.log")

class StreamStats:
    """
    Class for tracking streaming statistics
    """
    def __init__(self):
        self.start_time = time.time()
        self.total_records = 0
        self.total_batches = 0
        self.total_files = 0
        self.current_file = ""
        self.last_report_time = time.time()
        self.report_interval = 10  # seconds
    
    def update(self, records: int, batch: int = 1, file: Optional[str] = None):
        """Update the streaming statistics"""
        self.total_records += records
        self.total_batches += batch
        
        if file and file != self.current_file:
            self.total_files += 1
            self.current_file = file
    
    def should_report(self) -> bool:
        """Check if it's time to report statistics"""
        now = time.time()
        return now - self.last_report_time >= self.report_interval
    
    def report(self):
        """Print a statistics report"""
        now = time.time()
        elapsed = now - self.start_time
        records_per_second = self.total_records / elapsed if elapsed > 0 else 0
        
        logger.info(f"Streaming stats: {self.total_records} records, {self.total_batches} batches, {self.total_files} files")
        logger.info(f"Average rate: {records_per_second:.2f} records/second")
        
        self.last_report_time = now
        
    def final_report(self):
        """Print the final statistics report"""
        elapsed = time.time() - self.start_time
        records_per_second = self.total_records / elapsed if elapsed > 0 else 0
        
        logger.info("Stream completed")
        logger.info(f"Total records: {self.total_records}")
        logger.info(f"Total batches: {self.total_batches}")
        logger.info(f"Total files: {self.total_files}")
        logger.info(f"Total time: {elapsed:.2f} seconds")
        logger.info(f"Average rate: {records_per_second:.2f} records/second")

class StreamProducer:
    """
    Class for streaming Parquet data to Kafka
    """
    def __init__(
        self,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        topic: str = KAFKA_TOPIC,
        parquet_dir: str = PARQUET_DIR,
        batch_size: int = BATCH_SIZE,
        throttle_rate: float = THROTTLE_RATE
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.parquet_dir = parquet_dir
        self.batch_size = batch_size
        self.throttle_rate = throttle_rate
        self.producer = None
        self.reader = None
        self.stats = StreamStats()
        self.running = False
        self.interrupt_handler = self._setup_interrupt_handler()
    
    def _setup_interrupt_handler(self) -> Callable:
        """Set up a handler for SIGINT to gracefully shut down"""
        original_handler = signal.getsignal(signal.SIGINT)
        
        def handler(signum, frame):
            logger.info("Interrupt received, shutting down...")
            self.running = False
            
            # Give some time for cleanup
            time.sleep(1)
            
            # Call the original handler
            if callable(original_handler):
                original_handler(signum, frame)
        
        return handler
    
    def initialize(self) -> bool:
        """
        Initialize the producer and create the topic
        
        Returns:
            bool: True if initialization was successful
        """
        try:
            # Set up the interrupt handler only if in main thread
            import threading
            if threading.current_thread() is threading.main_thread():
                signal.signal(signal.SIGINT, self.interrupt_handler)
                logger.info("Set up interrupt handler in main thread")
            
            # Create the Kafka topic if it doesn't exist
            logger.info(f"Ensuring Kafka topic exists: {self.topic}")
            create_kafka_topic(self.bootstrap_servers, self.topic)
            
            # Start metrics collection
            self.start_metrics_collection()
            
            return True
        except Exception as e:
            logger.error(f"Error initializing producer: {str(e)}")
            record_system_error("producer", "initialization_error", str(e))
            return False
    
    def start_metrics_collection(self):
        """Start collecting metrics from this producer"""
        producer_metrics.start_collecting()
        logger.info("Started producer metrics collection")
    
    def update_metrics(self):
        """Update producer metrics with current statistics"""
        if hasattr(self, 'stats'):
            producer_metrics.update_producer_stats(
                total_records=self.stats.total_records,
                batches_sent=self.stats.total_batches,
                files_processed=self.stats.total_files,
                bytes_sent=self.stats.total_bytes,
                start_time=self.stats.start_time
            )
    
    def stream_to_kafka(self, max_records: int = None, max_files: int = None) -> bool:
        """
        Stream Parquet data to Kafka
        
        Args:
            max_records: Maximum number of records to send (optional)
            max_files: Maximum number of files to process (optional)
            
        Returns:
            bool: True if streaming completed successfully
        """
        try:
            self.is_running = True
            
            # Initialize the Kafka producer
            self.kafka_producer = get_kafka_producer(self.bootstrap_servers)
            
            # Initialize the Parquet reader
            parquet_reader = ParquetReader(self.parquet_dir)
            
            # Create statistics tracker
            self.stats = StreamStats()
            self.stats.start_time = datetime.now()
            
            # Start streaming files
            logger.info("Starting to stream Parquet data to Kafka")
            
            # Set up metrics thread for continuous updating
            def update_metrics_loop():
                while self.is_running:
                    try:
                        self.update_metrics()
                        time.sleep(1)
                    except Exception as e:
                        logger.error(f"Error updating metrics: {e}")
                        time.sleep(5)  # Wait longer on error
            
            metrics_thread = threading.Thread(target=update_metrics_loop)
            metrics_thread.daemon = True
            metrics_thread.start()
            
            result = parquet_reader.process_files(
                self.process_batch,
                batch_size=self.batch_size,
                max_records=max_records,
                max_files=max_files
            )
            
            # Wait for all messages to be delivered
            self.kafka_producer.flush()
            
            # Update metrics one last time
            self.update_metrics()
            
            # Set running status to false
            self.is_running = False
            
            # Log completion
            logger.info(f"Completed streaming to Kafka. {self.stats}")
            return result
            
        except Exception as e:
            logger.error(f"Error streaming to Kafka: {str(e)}")
            record_system_error("producer", "streaming_error", str(e))
            self.is_running = False
            return False
    
    def process_batch(self, batch_dict_list: List[Dict[str, Any]]) -> bool:
        """
        Process a batch of review records and send to Kafka
        
        Args:
            batch_dict_list: List of review dictionaries
            
        Returns:
            bool: True if batch was processed successfully
        """
        if not self.is_running:
            return False
        
        batch_size = len(batch_dict_list)
        batch_bytes = 0
        
        try:
            self.stats.total_batches += 1
            batch_start_time = datetime.now()
            
            # Send each record to Kafka
            for review in batch_dict_list:
                if not self.is_running:
                    return False
                
                # Add timestamp for latency tracking
                review["_producer_timestamp"] = datetime.now().isoformat()
                
                # Process the review to ensure all fields are properly encodable
                cleaned_review = self._sanitize_review_data(review)
                
                # Convert record to JSON
                review_json = json.dumps(cleaned_review)
                msg_bytes = review_json.encode("utf-8")
                batch_bytes += len(msg_bytes)
                
                # Send to Kafka
                self.kafka_producer.send(
                    self.topic,
                    key=str(cleaned_review.get("app_id", "unknown")).encode("utf-8"),
                    value=msg_bytes
                )
                
                # Update stats
                self.stats.total_records += 1
                
                # Check if we've reached the max records limit
                if self.stats.total_records % 100 == 0:
                    logger.info(f"Sent {self.stats.total_records} records to Kafka")
                    
                # Apply throttling if needed
                self._apply_throttling()
            
            # Update total bytes sent
            self.stats.total_bytes += batch_bytes
            
            # Calculate and record batch processing time
            batch_end_time = datetime.now()
            batch_time = (batch_end_time - batch_start_time).total_seconds()
            self.stats.batch_times.append(batch_time)
            
            # Log batch completion
            logger.debug(f"Sent batch of {batch_size} records ({batch_bytes} bytes) to Kafka in {batch_time:.3f} seconds")
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing batch: {str(e)}")
            record_system_error("producer", "batch_processing_error", str(e))
            return False
    
    def _sanitize_review_data(self, review: Dict[str, Any]) -> Dict[str, Any]:
        """
        Sanitize review data to ensure all fields are properly encodable
        
        Args:
            review: The original review dictionary
            
        Returns:
            A sanitized version of the review
        """
        sanitized = {}
        
        for key, value in review.items():
            if isinstance(value, (str, int, float, bool, type(None))):
                # Basic types can be directly included
                sanitized[key] = value
            elif isinstance(value, (list, tuple)):
                # For lists and tuples, process each item
                sanitized[key] = [
                    item if isinstance(item, (str, int, float, bool, type(None)))
                    else str(item) for item in value
                ]
            elif isinstance(value, dict):
                # Recursively process dictionaries
                sanitized[key] = self._sanitize_review_data(value)
            else:
                # Convert anything else to string
                sanitized[key] = str(value)
                
        return sanitized
    
    def cleanup(self):
        """Clean up resources"""
        logger.info("Cleaning up producer resources")
        
        # Close Kafka producer if it exists
        if hasattr(self, 'kafka_producer'):
            self.kafka_producer.flush()
            self.kafka_producer.close()
        
        # Final metrics update
        self.update_metrics()
        
        # Stop metrics collection
        producer_metrics.stop_collecting()
            
        logger.info("Producer resources cleaned up")

def create_and_start_producer(
    bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
    topic: str = KAFKA_TOPIC,
    parquet_dir: str = PARQUET_DIR,
    batch_size: int = BATCH_SIZE,
    throttle_rate: float = THROTTLE_RATE,
    max_records: Optional[int] = None,
    max_files: Optional[int] = None,
    sample_mode: bool = False,
    sample_size: int = 10,
    show_sample: bool = False
) -> bool:
    """
    Create and start a StreamProducer
    
    Args:
        bootstrap_servers (str): Kafka bootstrap servers
        topic (str): Kafka topic
        parquet_dir (str): Directory containing Parquet files
        batch_size (int): Batch size for reading Parquet files
        throttle_rate (float): Seconds to wait between batches
        max_records (int, optional): Maximum number of records to stream
        max_files (int, optional): Maximum number of files to process
        sample_mode (bool): Whether to run in sample mode
        sample_size (int): Number of records to stream in sample mode
        show_sample (bool): Whether to print sample data
    
    Returns:
        bool: True if the producer ran successfully
    """
    producer = StreamProducer(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        parquet_dir=parquet_dir,
        batch_size=batch_size,
        throttle_rate=throttle_rate
    )
    
    if not producer.initialize():
        logger.error("Failed to initialize producer")
        return False
    
    if sample_mode:
        return producer.stream_sample(num_records=sample_size, show_data=show_sample)
    else:
        return producer.stream_to_kafka(max_records=max_records, max_files=max_files)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Stream Parquet data to Kafka')
    parser.add_argument('--bootstrap-servers', default=KAFKA_BOOTSTRAP_SERVERS,
                        help='Kafka bootstrap servers')
    parser.add_argument('--topic', default=KAFKA_TOPIC,
                        help='Kafka topic')
    parser.add_argument('--parquet-dir', default=PARQUET_DIR,
                        help='Directory containing Parquet files')
    parser.add_argument('--batch-size', type=int, default=BATCH_SIZE,
                        help='Batch size for reading Parquet files')
    parser.add_argument('--throttle-rate', type=float, default=THROTTLE_RATE,
                        help='Seconds to wait between batches')
    parser.add_argument('--max-records', type=int, default=None,
                        help='Maximum number of records to stream')
    parser.add_argument('--max-files', type=int, default=None,
                        help='Maximum number of files to process')
    parser.add_argument('--sample', action='store_true',
                        help='Run in sample mode (stream a small sample of records)')
    parser.add_argument('--sample-size', type=int, default=10,
                        help='Number of records to stream in sample mode')
    parser.add_argument('--show-sample', action='store_true',
                        help='Print sample data when in sample mode')
    
    args = parser.parse_args()
    
    logger.info(f"Using {'Mock' if USE_MOCK_KAFKA else 'Real'} Kafka implementation")
    
    success = create_and_start_producer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        parquet_dir=args.parquet_dir,
        batch_size=args.batch_size,
        throttle_rate=args.throttle_rate,
        max_records=args.max_records,
        max_files=args.max_files,
        sample_mode=args.sample,
        sample_size=args.sample_size,
        show_sample=args.show_sample
    )
    
    if success:
        print("Streaming completed successfully")
        exit(0)
    else:
        print("Streaming failed")
        exit(1) 