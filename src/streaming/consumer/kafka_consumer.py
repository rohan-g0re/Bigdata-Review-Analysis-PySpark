"""
Kafka Consumer for processing Steam review data from Kafka stream
"""
import json
import time
import signal
import threading
import os
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime
import pandas as pd

from src.streaming.utils.logging_config import setup_logger
from src.streaming.utils.kafka_utils import create_consumer, consume_messages
from src.streaming.config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    KAFKA_CONSUMER_GROUP,
    KAFKA_AUTO_OFFSET_RESET,
    STREAM_RESULTS_DIR,
    USE_MOCK_KAFKA
)

# Add import for metrics
from src.streaming.monitoring.metrics_collector import consumer_metrics, record_system_error

logger = setup_logger("kafka_consumer", "kafka_consumer.log")

class StreamingStats:
    """
    Class for tracking streaming statistics
    """
    def __init__(self):
        self.start_time = time.time()
        self.total_records = 0
        self.last_report_time = time.time()
        self.report_interval = 10  # seconds
    
    def update(self, records: int):
        """Update the streaming statistics"""
        self.total_records += records
    
    def should_report(self) -> bool:
        """Check if it's time to report statistics"""
        now = time.time()
        return now - self.last_report_time >= self.report_interval
    
    def report(self):
        """Print a statistics report"""
        now = time.time()
        elapsed = now - self.start_time
        records_per_second = self.total_records / elapsed if elapsed > 0 else 0
        
        logger.info(f"Consumer stats: {self.total_records} records processed")
        logger.info(f"Average rate: {records_per_second:.2f} records/second")
        
        self.last_report_time = now
        
    def final_report(self):
        """Print the final statistics report"""
        elapsed = time.time() - self.start_time
        records_per_second = self.total_records / elapsed if elapsed > 0 else 0
        
        logger.info("Consumption completed")
        logger.info(f"Total records: {self.total_records}")
        logger.info(f"Total time: {elapsed:.2f} seconds")
        logger.info(f"Average rate: {records_per_second:.2f} records/second")

class StreamAnalytics:
    """
    Class to perform real-time analytics on streaming data
    """
    def __init__(self, results_dir: str = STREAM_RESULTS_DIR):
        self.results_dir = results_dir
        self.review_counts = {}  # game_id -> count
        self.sentiment_scores = {}  # game_id -> list of scores
        self.timestamp_counts = {}  # hour -> count
        self.recent_reviews = []  # list to store recent reviews
        self.max_recent_reviews = 100  # max number of recent reviews to store
        self.lock = threading.Lock()  # thread safety for analytics
        
        # Ensure results directory exists
        os.makedirs(self.results_dir, exist_ok=True)
    
    def update(self, review: Dict[str, Any]):
        """Update analytics with a new review"""
        with self.lock:
            # Extract needed fields
            game_id = review.get('app_id')
            
            # If app_id is missing, try to use the game field
            if game_id is None or pd.isna(game_id):
                game_id = review.get('game')
                
            # Still no game_id, use "unknown"
            if game_id is None or pd.isna(game_id):
                game_id = "unknown"
                
            # Convert to string to ensure it can be used as a key
            game_id = str(game_id)
            
            sentiment = 1 if review.get('recommended', False) else 0
            timestamp = review.get('timestamp_created')
            
            # Update review counts per game
            if game_id not in self.review_counts:
                self.review_counts[game_id] = 0
            self.review_counts[game_id] += 1
            
            # Update sentiment scores
            if game_id not in self.sentiment_scores:
                self.sentiment_scores[game_id] = []
            self.sentiment_scores[game_id].append(sentiment)
            
            # Update timestamp counts (by hour)
            try:
                if timestamp:
                    # Convert timestamp to datetime if it's a string
                    if isinstance(timestamp, str):
                        dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    else:
                        dt = datetime.fromtimestamp(timestamp)
                    
                    hour_key = dt.strftime('%Y-%m-%d %H:00')
                    if hour_key not in self.timestamp_counts:
                        self.timestamp_counts[hour_key] = 0
                    self.timestamp_counts[hour_key] += 1
            except (ValueError, TypeError) as e:
                logger.warning(f"Error processing timestamp: {e}")
            
            # Update recent reviews
            self.recent_reviews.append({
                'app_id': game_id,
                'recommended': review.get('recommended', False),
                'review_text': review.get('review', '')[:200] + '...' if len(review.get('review', '')) > 200 else review.get('review', ''),
                'timestamp': timestamp
            })
            
            # Keep only the most recent reviews
            if len(self.recent_reviews) > self.max_recent_reviews:
                self.recent_reviews = self.recent_reviews[-self.max_recent_reviews:]
    
    def save_results(self):
        """Save analytics results to files for dashboard consumption"""
        with self.lock:
            try:
                # Save top games by review count
                if self.review_counts:
                    top_games = pd.DataFrame({
                        'game_id': list(self.review_counts.keys()),
                        'review_count': list(self.review_counts.values())
                    }).sort_values('review_count', ascending=False).head(20)
                    
                    top_games.to_csv(os.path.join(self.results_dir, 'top_games.csv'), index=False)
                
                # Save sentiment analysis
                sentiment_data = []
                for game_id, scores in self.sentiment_scores.items():
                    if len(scores) > 0:  # Include games with at least one review
                        avg_sentiment = sum(scores) / len(scores)
                        sentiment_data.append({
                            'game_id': game_id,
                            'review_count': len(scores),
                            'avg_sentiment': avg_sentiment
                        })
                
                if sentiment_data:
                    sentiment_df = pd.DataFrame(sentiment_data).sort_values('review_count', ascending=False)
                    sentiment_df.to_csv(os.path.join(self.results_dir, 'sentiment_analysis.csv'), index=False)
                
                # Save time distribution
                if self.timestamp_counts:
                    time_df = pd.DataFrame({
                        'hour': list(self.timestamp_counts.keys()),
                        'count': list(self.timestamp_counts.values())
                    }).sort_values('hour')
                    
                    time_df.to_csv(os.path.join(self.results_dir, 'time_distribution.csv'), index=False)
                
                # Save recent reviews
                if self.recent_reviews:
                    recent_df = pd.DataFrame(self.recent_reviews)
                    recent_df.to_csv(os.path.join(self.results_dir, 'recent_reviews.csv'), index=False)
                
                logger.info(f"Saved analytics results to {self.results_dir}")
                
            except Exception as e:
                logger.error(f"Error saving analytics results: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())

class StreamConsumer:
    """
    Class for consuming Steam review data from Kafka
    """
    def __init__(
        self,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        topic: str = KAFKA_TOPIC,
        group_id: str = KAFKA_CONSUMER_GROUP,
        auto_offset_reset: str = KAFKA_AUTO_OFFSET_RESET,
        results_dir: str = STREAM_RESULTS_DIR
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.auto_offset_reset = auto_offset_reset
        self.results_dir = results_dir
        self.consumer = None
        self.stats = StreamingStats()
        self.analytics = StreamAnalytics(results_dir=results_dir)
        self.running = False
        self.save_interval = 3  # seconds between saving results
        self.last_save_time = time.time()
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
        Initialize the consumer
        
        Returns:
            bool: True if initialization was successful
        """
        try:
            # Set up the interrupt handler
            import threading
            if threading.current_thread() is threading.main_thread():
                signal.signal(signal.SIGINT, self.interrupt_handler)
                logger.info("Set up interrupt handler in main thread")
            
            # Set up metrics collection
            self.start_metrics_collection()
            
            # Initialize analytics
            self.analytics = StreamAnalytics(self.results_dir)
            # Create analytics directory if it doesn't exist
            os.makedirs(self.results_dir, exist_ok=True)
            logger.info(f"Analytics results will be saved to {self.results_dir}")
            
            # Create the results directory if it doesn't exist
            os.makedirs(self.results_dir, exist_ok=True)
            
            return True
        except Exception as e:
            logger.error(f"Error initializing consumer: {str(e)}")
            record_system_error("consumer", "initialization_error", str(e))
            return False
    
    def start_metrics_collection(self):
        """Start collecting metrics from this consumer"""
        self.start_time = datetime.now()
        self.processed_count = 0
        self.error_count = 0
        self.processed_games = set()
        self.processing_times = []
        
        consumer_metrics.start_collecting()
        logger.info("Started consumer metrics collection")
    
    def update_metrics(self):
        """Update consumer metrics with current statistics"""
        avg_processing_time = sum(self.processing_times) / len(self.processing_times) if self.processing_times else 0
        
        consumer_metrics.update_consumer_stats(
            messages_processed=self.processed_count,
            games_analyzed=len(self.processed_games),
            processing_time_ms=avg_processing_time,
            start_time=self.start_time,
            error_count=self.error_count
        )
    
    def consume_from_kafka(self, max_messages: Optional[int] = None) -> bool:
        """
        Consume messages from Kafka and perform analytics
        
        Args:
            max_messages: Maximum number of messages to consume (for testing)
            
        Returns:
            bool: True if consumption completed successfully
        """
        try:
            self.is_running = True
            
            # Create consumer
            consumer = create_consumer(
                self.bootstrap_servers,
                self.topic,
                self.group_id,
                self.auto_offset_reset
            )
            
            # Set up metrics update thread
            def update_metrics_loop():
                while self.is_running:
                    try:
                        self.update_metrics()
                        time.sleep(1)
                    except Exception as e:
                        logger.error(f"Error updating metrics: {e}")
                        time.sleep(5)  # Wait longer on error
            
            # Start metrics update thread
            metrics_thread = threading.Thread(target=update_metrics_loop)
            metrics_thread.daemon = True
            metrics_thread.start()
            
            # Process messages
            message_count = 0
            
            logger.info(f"Starting to consume messages from topic {self.topic}")
            
            for message in consumer:
                if not self.is_running:
                    logger.info("Consumer stopping due to interrupt")
                    break
                
                try:
                    # Process the message
                    message_time_start = time.time()
                    self._process_message(message)
                    message_time_end = time.time()
                    
                    # Track processing time
                    processing_time_ms = (message_time_end - message_time_start) * 1000
                    self.processing_times.append(processing_time_ms)
                    
                    # Keep only the latest 100 processing times
                    if len(self.processing_times) > 100:
                        self.processing_times = self.processing_times[-100:]
                    
                    # Increment counter
                    message_count += 1
                    self.processed_count += 1
                    
                    # Log progress
                    if message_count % 100 == 0:
                        logger.info(f"Processed {message_count} messages")
                    
                    # Check if we've reached the maximum
                    if max_messages and message_count >= max_messages:
                        logger.info(f"Reached maximum message count: {max_messages}")
                        break
                    
                except Exception as e:
                    logger.error(f"Error processing message: {str(e)}")
                    self.error_count += 1
                    record_system_error("consumer", "message_processing_error", str(e))
            
            # Save the analytics results
            logger.info("Saving analytics results")
            self.analytics.save_results()
            
            # Final metrics update
            self.update_metrics()
            
            # Stop running flag
            self.is_running = False
            
            logger.info(f"Consumer completed. Processed {message_count} messages")
            return True
            
        except Exception as e:
            logger.error(f"Error consuming from Kafka: {str(e)}")
            record_system_error("consumer", "consumption_error", str(e))
            self.is_running = False
            return False
    
    def _process_message(self, message):
        """
        Process a Kafka message
        
        Args:
            message: Kafka message object
        """
        try:
            # Decode the message value with error handling
            try:
                value = json.loads(message.value.decode("utf-8"))
            except UnicodeDecodeError:
                # Try with error handling - replace invalid bytes
                value = json.loads(message.value.decode("utf-8", errors="replace"))
                logger.warning("Handled invalid UTF-8 sequence in message data")
            
            # Extract producer timestamp for latency tracking if available
            if "_producer_timestamp" in value:
                try:
                    producer_timestamp = datetime.fromisoformat(value["_producer_timestamp"])
                    consumer_timestamp = datetime.now()
                    
                    # Record latency
                    consumer_metrics.record_latency(producer_timestamp, consumer_timestamp)
                except Exception as ex:
                    logger.warning(f"Could not parse producer timestamp: {ex}")
            
            # Process with analytics
            app_id = value.get("app_id")
            if app_id:
                self.processed_games.add(app_id)
            
            self.analytics.update(value)
            
        except Exception as e:
            logger.error(f"Error processing message data: {str(e)}")
            self.error_count += 1
            record_system_error("consumer", "message_parsing_error", str(e))
    
    def cleanup(self):
        """Clean up resources"""
        logger.info("Cleaning up consumer resources")
        
        # Final metrics update
        self.update_metrics()
        
        # Stop metrics collection
        consumer_metrics.stop_collecting()
        
        logger.info("Consumer resources cleaned up")

def create_and_start_consumer(
    bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
    topic: str = KAFKA_TOPIC,
    group_id: str = KAFKA_CONSUMER_GROUP,
    auto_offset_reset: str = KAFKA_AUTO_OFFSET_RESET,
    results_dir: str = STREAM_RESULTS_DIR,
    max_messages: Optional[int] = None
) -> bool:
    """
    Create and start a Kafka consumer in one call
    
    Args:
        bootstrap_servers (str): Kafka bootstrap servers
        topic (str): Kafka topic to consume from
        group_id (str): Consumer group ID
        auto_offset_reset (str): Where to start consuming (earliest/latest)
        results_dir (str): Directory to save results
        max_messages (int, optional): Maximum number of messages to consume
        
    Returns:
        bool: True if the consumer completed successfully
    """
    consumer = StreamConsumer(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        results_dir=results_dir
    )
    
    if not consumer.initialize():
        logger.error("Failed to initialize consumer")
        return False
    
    return consumer.consume_from_kafka(max_messages=max_messages)


if __name__ == "__main__":
    # Start consuming
    create_and_start_consumer() 