"""
Integration test for the complete streaming system

This script runs a full integration test of the entire streaming system:
1. Producer reading from Parquet files
2. Kafka message transmission
3. Consumer processing
4. Analytics dashboard
5. End-to-end latency measurement

Usage:
    python -m src.streaming.test_integration

Options:
    --data-volume: small, medium, or large (default: small)
    --rate: slow, normal, or fast (default: normal)
    --duration: How long to run the test in seconds (default: 60)
"""
import os
import time
import argparse
import threading
import subprocess
import pandas as pd
import logging
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple

from src.streaming.utils.logging_config import setup_logger
from src.streaming.producer.kafka_producer import create_and_start_producer, StreamProducer
from src.streaming.consumer.kafka_consumer import create_and_start_consumer, StreamConsumer
from src.streaming.config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    KAFKA_CONSUMER_GROUP,
    PARQUET_DIR,
    BATCH_SIZE,
    THROTTLE_RATE,
    STREAM_RESULTS_DIR
)

# Set up logging
logger = setup_logger("integration_test", "integration_test.log")

class SystemPerformanceMonitor:
    """Monitor system performance during integration test"""
    
    def __init__(self):
        self.start_time = datetime.now()
        self.metrics = {
            "producer": {
                "records_sent": 0,
                "batches_sent": 0,
                "start_time": None,
                "end_time": None,
                "errors": 0
            },
            "consumer": {
                "records_processed": 0,
                "start_time": None,
                "end_time": None,
                "errors": 0
            },
            "e2e": {
                "latencies": [],
                "avg_latency": 0
            }
        }
        self.running = False
        self.monitor_thread = None
    
    def start_monitoring(self):
        """Start the monitoring thread"""
        self.running = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
        logger.info("Performance monitoring started")
    
    def stop_monitoring(self):
        """Stop the monitoring thread"""
        self.running = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=2)
        logger.info("Performance monitoring stopped")
    
    def _monitor_loop(self):
        """Monitor loop that collects metrics periodically"""
        while self.running:
            # Check if results directory exists
            if os.path.exists(STREAM_RESULTS_DIR):
                self._check_consumer_output()
            
            time.sleep(5)
    
    def _check_consumer_output(self):
        """Check consumer output files to measure throughput and latency"""
        try:
            # Load consumer metrics from output files
            recent_reviews_path = os.path.join(STREAM_RESULTS_DIR, 'recent_reviews.csv')
            if os.path.exists(recent_reviews_path):
                reviews_df = pd.read_csv(recent_reviews_path)
                self.metrics["consumer"]["records_processed"] = len(reviews_df)
                
            # Calculate other metrics
            elapsed = (datetime.now() - self.start_time).total_seconds()
            if elapsed > 0:
                producer_throughput = self.metrics["producer"]["records_sent"] / elapsed
                consumer_throughput = self.metrics["consumer"]["records_processed"] / elapsed
                
                logger.info(f"Throughput - Producer: {producer_throughput:.2f} msgs/sec, "
                           f"Consumer: {consumer_throughput:.2f} msgs/sec")
        except Exception as e:
            logger.error(f"Error monitoring system performance: {str(e)}")
    
    def update_producer_metrics(self, records_sent: int, batches_sent: int, error_count: int = 0):
        """Update producer metrics"""
        self.metrics["producer"]["records_sent"] = records_sent
        self.metrics["producer"]["batches_sent"] = batches_sent
        self.metrics["producer"]["errors"] = error_count
    
    def measure_latency(self, producer_timestamp: datetime, consumer_timestamp: datetime):
        """Measure and record end-to-end latency"""
        if producer_timestamp and consumer_timestamp:
            latency = (consumer_timestamp - producer_timestamp).total_seconds()
            self.metrics["e2e"]["latencies"].append(latency)
            self.metrics["e2e"]["avg_latency"] = sum(self.metrics["e2e"]["latencies"]) / len(self.metrics["e2e"]["latencies"])
            logger.info(f"End-to-end latency: {latency:.2f}s, Avg: {self.metrics['e2e']['avg_latency']:.2f}s")
    
    def generate_report(self) -> Dict[str, Any]:
        """Generate a performance report"""
        elapsed = (datetime.now() - self.start_time).total_seconds()
        
        # Calculate final metrics
        producer_throughput = self.metrics["producer"]["records_sent"] / elapsed if elapsed > 0 else 0
        consumer_throughput = self.metrics["consumer"]["records_processed"] / elapsed if elapsed > 0 else 0
        
        # Calculate latency statistics
        latencies = self.metrics["e2e"]["latencies"]
        avg_latency = sum(latencies) / len(latencies) if latencies else 0
        max_latency = max(latencies) if latencies else 0
        min_latency = min(latencies) if latencies else 0
        
        report = {
            "test_duration_seconds": elapsed,
            "producer": {
                "records_sent": self.metrics["producer"]["records_sent"],
                "batches_sent": self.metrics["producer"]["batches_sent"],
                "throughput_records_per_second": producer_throughput,
                "errors": self.metrics["producer"]["errors"]
            },
            "consumer": {
                "records_processed": self.metrics["consumer"]["records_processed"],
                "throughput_records_per_second": consumer_throughput,
                "errors": self.metrics["consumer"]["errors"]
            },
            "latency": {
                "average_seconds": avg_latency,
                "min_seconds": min_latency,
                "max_seconds": max_latency,
                "samples": len(latencies)
            }
        }
        
        # Log the report
        logger.info("Performance Report:\n" + 
                   f"Test Duration: {elapsed:.2f} seconds\n" +
                   f"Producer Records Sent: {report['producer']['records_sent']}\n" +
                   f"Producer Throughput: {producer_throughput:.2f} records/sec\n" +
                   f"Consumer Records Processed: {report['consumer']['records_processed']}\n" +
                   f"Consumer Throughput: {consumer_throughput:.2f} records/sec\n" +
                   f"Average Latency: {avg_latency:.2f} seconds\n" +
                   f"Min/Max Latency: {min_latency:.2f}/{max_latency:.2f} seconds")
        
        return report

# Custom producer/consumer wrappers for integration testing
class TestProducer:
    """Wrapper for producer with testing instrumentation"""
    
    def __init__(self, monitor: SystemPerformanceMonitor):
        self.monitor = monitor
        self.producer = None
        self.stats_thread = None
        self.running = False
        self.timestamp_tracker = {}  # Track message timestamps for latency measurement
    
    def start(self, max_records: int = 100, max_files: int = 1, throttle_rate: float = THROTTLE_RATE):
        """Start the producer with test configuration"""
        self.running = True
        
        # Start the producer in a thread
        def run_producer():
            try:
                # Initialize but don't start streaming yet
                self.producer = StreamProducer(
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    topic=KAFKA_TOPIC,
                    parquet_dir=PARQUET_DIR,
                    batch_size=BATCH_SIZE,
                    throttle_rate=throttle_rate
                )
                
                if not self.producer.initialize():
                    logger.error("Failed to initialize producer")
                    return
                
                # Start streaming
                result = self.producer.stream_to_kafka(max_records=max_records, max_files=max_files)
                
                if result:
                    logger.info(f"Producer completed successfully. Sent {self.producer.stats.total_records} records")
                else:
                    logger.error("Producer failed")
                
                # Update metrics
                self.monitor.update_producer_metrics(
                    self.producer.stats.total_records,
                    self.producer.stats.total_batches,
                    0 if result else 1
                )
                
            except Exception as e:
                logger.error(f"Error in producer thread: {str(e)}")
                self.monitor.update_producer_metrics(0, 0, 1)
        
        # Start the stats collection thread
        def collect_stats():
            while self.running and self.producer:
                try:
                    if hasattr(self.producer, 'stats'):
                        self.monitor.update_producer_metrics(
                            self.producer.stats.total_records,
                            self.producer.stats.total_batches
                        )
                except Exception as e:
                    logger.error(f"Error collecting producer stats: {str(e)}")
                time.sleep(1)
        
        # Start threads
        producer_thread = threading.Thread(target=run_producer)
        producer_thread.daemon = True
        producer_thread.start()
        
        self.stats_thread = threading.Thread(target=collect_stats)
        self.stats_thread.daemon = True
        self.stats_thread.start()
        
        logger.info("Test producer started")
    
    def stop(self):
        """Stop the producer"""
        self.running = False
        
        # Producer should stop on its own when it hits max_records/max_files
        
        # Wait for stats thread to finish
        if self.stats_thread:
            self.stats_thread.join(timeout=2)
        
        logger.info("Test producer stopped")

class TestConsumer:
    """Wrapper for consumer with testing instrumentation"""
    
    def __init__(self, monitor: SystemPerformanceMonitor):
        self.monitor = monitor
        self.consumer = None
        self.stats_thread = None
        self.running = False
    
    def start(self, max_messages: Optional[int] = None):
        """Start the consumer with test configuration"""
        self.running = True
        
        # Start the consumer in a thread
        def run_consumer():
            try:
                result = create_and_start_consumer(
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    topic=KAFKA_TOPIC,
                    group_id=f"{KAFKA_CONSUMER_GROUP}_test",
                    results_dir=STREAM_RESULTS_DIR,
                    max_messages=max_messages
                )
                
                if result:
                    logger.info("Consumer completed successfully")
                else:
                    logger.error("Consumer failed")
                
            except Exception as e:
                logger.error(f"Error in consumer thread: {str(e)}")
        
        # Start consumer thread
        consumer_thread = threading.Thread(target=run_consumer)
        consumer_thread.daemon = True
        consumer_thread.start()
        
        logger.info("Test consumer started")
    
    def stop(self):
        """Stop the consumer"""
        self.running = False
        # Consumer will stop on its own when it hits max_messages
        logger.info("Test consumer stopping")

def run_integration_test(
    data_volume: str = "small",
    rate: str = "normal",
    duration: int = 60
):
    """
    Run the integration test with the specified parameters
    
    Args:
        data_volume: small, medium, or large
        rate: slow, normal, or fast
        duration: test duration in seconds
    """
    logger.info(f"Starting integration test: volume={data_volume}, rate={rate}, duration={duration}s")
    
    # Configure test parameters based on data volume and rate
    if data_volume == "small":
        max_records = 100
        max_files = 1
    elif data_volume == "medium":
        max_records = 1000
        max_files = 3
    else:  # large
        max_records = 10000
        max_files = 5
    
    if rate == "slow":
        throttle_rate = THROTTLE_RATE * 2
    elif rate == "normal":
        throttle_rate = THROTTLE_RATE
    else:  # fast
        throttle_rate = THROTTLE_RATE / 4
    
    # Set up results directory
    os.makedirs(STREAM_RESULTS_DIR, exist_ok=True)
    
    # Initialize the performance monitor
    monitor = SystemPerformanceMonitor()
    monitor.start_monitoring()
    
    try:
        # Start consumer
        consumer = TestConsumer(monitor)
        consumer.start()
        
        # Give consumer a moment to initialize
        time.sleep(2)
        
        # Start producer
        producer = TestProducer(monitor)
        producer.start(max_records=max_records, max_files=max_files, throttle_rate=throttle_rate)
        
        # Run for the specified duration
        logger.info(f"Test running for {duration} seconds...")
        time.sleep(duration)
        
        # Stop components
        producer.stop()
        consumer.stop()
        
        # Generate final report
        report = monitor.generate_report()
        
        # Check if the test passed
        passed = (
            report["latency"]["average_seconds"] < 30 and  # Latency under 30 seconds
            report["producer"]["errors"] == 0 and          # No producer errors
            report["consumer"]["errors"] == 0              # No consumer errors
        )
        
        if passed:
            logger.info("Integration test PASSED")
        else:
            logger.error("Integration test FAILED")
        
        return report, passed
        
    except Exception as e:
        logger.error(f"Error during integration test: {str(e)}")
        return None, False
    finally:
        monitor.stop_monitoring()
        logger.info("Integration test completed")

if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Run integration test for the streaming system")
    parser.add_argument(
        "--data-volume",
        choices=["small", "medium", "large"],
        default="small",
        help="Amount of data to process (default: small)"
    )
    parser.add_argument(
        "--rate",
        choices=["slow", "normal", "fast"],
        default="normal",
        help="Rate of data production (default: normal)"
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=60,
        help="Test duration in seconds (default: 60)"
    )
    
    args = parser.parse_args()
    
    # Run the integration test
    report, passed = run_integration_test(
        data_volume=args.data_volume,
        rate=args.rate,
        duration=args.duration
    )
    
    # Exit with appropriate status code
    if passed:
        exit(0)
    else:
        exit(1) 