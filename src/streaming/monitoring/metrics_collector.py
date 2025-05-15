"""
Metrics collector for streaming components

This module provides classes and functions to collect and report
performance metrics from the producer and consumer components.
"""
import os
import json
import time
import threading
import logging
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("metrics_collector")

class MetricsCollector:
    """Base metrics collector class"""
    
    def __init__(self, component_name: str, metrics_dir: str = "data/metrics"):
        self.component_name = component_name
        self.metrics_dir = metrics_dir
        self.metrics = {
            "start_time": datetime.now().isoformat(),
            "last_update_time": datetime.now().isoformat(),
            "component_name": component_name,
            "processed_count": 0,
            "error_count": 0,
            "throughput": 0.0,
            "latency": 0.0,
            "additional_metrics": {}
        }
        self.running = False
        self.metrics_thread = None
        
        # Create metrics directory if it doesn't exist
        os.makedirs(metrics_dir, exist_ok=True)
        
        # Initialize the metrics file
        self._save_metrics()
    
    def start_collecting(self):
        """Start the metrics collection thread"""
        self.running = True
        self.metrics_thread = threading.Thread(target=self._metrics_loop)
        self.metrics_thread.daemon = True
        self.metrics_thread.start()
        logger.info(f"Started metrics collection for {self.component_name}")
    
    def stop_collecting(self):
        """Stop the metrics collection thread"""
        self.running = False
        if self.metrics_thread:
            self.metrics_thread.join(timeout=2)
        logger.info(f"Stopped metrics collection for {self.component_name}")
    
    def _metrics_loop(self):
        """Background thread that saves metrics periodically"""
        while self.running:
            try:
                # Save metrics to disk
                self._save_metrics()
                
                # Sleep for a while
                time.sleep(5)
                
            except Exception as e:
                logger.error(f"Error in metrics collection loop: {str(e)}")
                time.sleep(10)  # Wait longer on error
    
    def update_metrics(self, metrics_update: Dict[str, Any]):
        """
        Update metrics with new values
        
        Args:
            metrics_update: Dict with metrics to update
        """
        # Update last update time
        self.metrics["last_update_time"] = datetime.now().isoformat()
        
        # Update regular metrics
        for key in ["processed_count", "error_count", "throughput", "latency"]:
            if key in metrics_update:
                self.metrics[key] = metrics_update[key]
        
        # Update additional metrics
        if "additional_metrics" in metrics_update:
            self.metrics["additional_metrics"].update(metrics_update["additional_metrics"])
    
    def _save_metrics(self):
        """Save metrics to disk"""
        try:
            metrics_file = os.path.join(self.metrics_dir, f"{self.component_name}_metrics.json")
            with open(metrics_file, 'w') as f:
                json.dump(self.metrics, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving metrics: {str(e)}")
    
    def calculate_throughput(self, processed_count: int, elapsed_seconds: float) -> float:
        """
        Calculate throughput in items per second
        
        Args:
            processed_count: Number of items processed
            elapsed_seconds: Time elapsed in seconds
            
        Returns:
            Throughput in items per second
        """
        if elapsed_seconds > 0:
            return processed_count / elapsed_seconds
        return 0.0

class ProducerMetricsCollector(MetricsCollector):
    """Metrics collector for the producer component"""
    
    def __init__(self, metrics_dir: str = "data/metrics"):
        super().__init__("producer", metrics_dir)
        self.metrics["additional_metrics"] = {
            "files_processed": 0,
            "batches_sent": 0,
            "total_bytes_sent": 0
        }
    
    def update_producer_stats(
        self, 
        total_records: int, 
        batches_sent: int, 
        files_processed: int, 
        bytes_sent: int,
        start_time: datetime
    ):
        """
        Update producer-specific metrics
        
        Args:
            total_records: Total number of records sent
            batches_sent: Total number of batches sent
            files_processed: Total number of files processed
            bytes_sent: Total bytes sent
            start_time: Time when the producer started
        """
        now = datetime.now()
        elapsed_seconds = (now - start_time).total_seconds()
        throughput = self.calculate_throughput(total_records, elapsed_seconds)
        
        metrics_update = {
            "processed_count": total_records,
            "throughput": throughput,
            "additional_metrics": {
                "files_processed": files_processed,
                "batches_sent": batches_sent,
                "total_bytes_sent": bytes_sent,
                "avg_batch_size": total_records / batches_sent if batches_sent > 0 else 0,
                "elapsed_seconds": elapsed_seconds
            }
        }
        
        self.update_metrics(metrics_update)

class ConsumerMetricsCollector(MetricsCollector):
    """Metrics collector for the consumer component"""
    
    def __init__(self, metrics_dir: str = "data/metrics"):
        super().__init__("consumer", metrics_dir)
        self.metrics["additional_metrics"] = {
            "messages_processed": 0,
            "games_analyzed": 0,
            "processing_time_ms": 0.0
        }
        self.latency_samples = []
    
    def update_consumer_stats(
        self, 
        messages_processed: int, 
        games_analyzed: int, 
        processing_time_ms: float,
        start_time: datetime,
        error_count: int = 0
    ):
        """
        Update consumer-specific metrics
        
        Args:
            messages_processed: Number of Kafka messages processed
            games_analyzed: Number of unique games analyzed
            processing_time_ms: Average processing time per message in milliseconds
            start_time: Time when the consumer started
            error_count: Number of errors encountered
        """
        now = datetime.now()
        elapsed_seconds = (now - start_time).total_seconds()
        throughput = self.calculate_throughput(messages_processed, elapsed_seconds)
        
        metrics_update = {
            "processed_count": messages_processed,
            "error_count": error_count,
            "throughput": throughput,
            "additional_metrics": {
                "games_analyzed": games_analyzed,
                "processing_time_ms": processing_time_ms,
                "elapsed_seconds": elapsed_seconds
            }
        }
        
        self.update_metrics(metrics_update)
    
    def record_latency(self, producer_timestamp: datetime, consumer_timestamp: datetime):
        """
        Record end-to-end latency for a message
        
        Args:
            producer_timestamp: When the message was produced
            consumer_timestamp: When the message was consumed
        """
        if producer_timestamp and consumer_timestamp:
            latency_seconds = (consumer_timestamp - producer_timestamp).total_seconds()
            self.latency_samples.append(latency_seconds)
            
            # Keep only the latest 100 samples
            if len(self.latency_samples) > 100:
                self.latency_samples = self.latency_samples[-100:]
            
            # Update average latency
            avg_latency = sum(self.latency_samples) / len(self.latency_samples)
            self.metrics["latency"] = avg_latency
            
            # Save latency metrics to a separate file
            self._save_latency_metrics()
    
    def _save_latency_metrics(self):
        """Save latency metrics to a separate file"""
        try:
            latency_metrics = {
                "timestamp": datetime.now().isoformat(),
                "latency": self.metrics["latency"],
                "samples": len(self.latency_samples),
                "min_latency": min(self.latency_samples) if self.latency_samples else 0,
                "max_latency": max(self.latency_samples) if self.latency_samples else 0
            }
            
            latency_file = os.path.join(self.metrics_dir, "latency_metrics.json")
            with open(latency_file, 'w') as f:
                json.dump(latency_metrics, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving latency metrics: {str(e)}")

class QueueMetricsCollector(MetricsCollector):
    """Metrics collector for the Kafka queue"""
    
    def __init__(self, metrics_dir: str = "data/metrics"):
        super().__init__("queue", metrics_dir)
        self.metrics["additional_metrics"] = {
            "partition_offsets": {},
            "consumer_lag": 0
        }
    
    def update_queue_stats(
        self, 
        queue_size: int, 
        partition_offsets: Dict[int, int], 
        consumer_lag: int
    ):
        """
        Update queue-specific metrics
        
        Args:
            queue_size: Current number of messages in the queue
            partition_offsets: Current offsets for each partition
            consumer_lag: Consumer lag in messages
        """
        metrics_update = {
            "processed_count": queue_size,
            "additional_metrics": {
                "partition_offsets": partition_offsets,
                "consumer_lag": consumer_lag
            }
        }
        
        self.update_metrics(metrics_update)
        
        # Save queue size to a separate metrics file
        self._save_queue_metrics(queue_size, consumer_lag)
    
    def _save_queue_metrics(self, queue_size: int, consumer_lag: int):
        """Save queue metrics to a separate file"""
        try:
            queue_metrics = {
                "timestamp": datetime.now().isoformat(),
                "queue_size": queue_size,
                "consumer_lag": consumer_lag
            }
            
            queue_file = os.path.join(self.metrics_dir, "queue_metrics.json")
            with open(queue_file, 'w') as f:
                json.dump(queue_metrics, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving queue metrics: {str(e)}")

class ErrorMetricsCollector(MetricsCollector):
    """Metrics collector for system errors"""
    
    def __init__(self, metrics_dir: str = "data/metrics"):
        super().__init__("error", metrics_dir)
        self.metrics["additional_metrics"] = {
            "error_details": [],
            "error_types": {}
        }
        self.error_count = 0
    
    def record_error(self, component: str, error_type: str, error_message: str, severity: str = "error"):
        """
        Record an error occurrence
        
        Args:
            component: Component where the error occurred
            error_type: Type of error
            error_message: Error message
            severity: Error severity (info, warning, error, critical)
        """
        # Increment error count
        self.error_count += 1
        
        # Create error details entry
        error_details = {
            "timestamp": datetime.now().isoformat(),
            "component": component,
            "error_type": error_type,
            "error_message": error_message,
            "severity": severity
        }
        
        # Update error metrics
        self.metrics["error_count"] = self.error_count
        
        # Add to error details list (keep only the latest 20)
        error_details_list = self.metrics["additional_metrics"]["error_details"]
        error_details_list.append(error_details)
        self.metrics["additional_metrics"]["error_details"] = error_details_list[-20:]
        
        # Update error type counts
        error_types = self.metrics["additional_metrics"]["error_types"]
        error_types[error_type] = error_types.get(error_type, 0) + 1
        self.metrics["additional_metrics"]["error_types"] = error_types
        
        # Save error metrics
        self._save_error_metrics()
    
    def _save_error_metrics(self):
        """Save error metrics to a separate file"""
        try:
            error_metrics = {
                "timestamp": datetime.now().isoformat(),
                "error_count": self.error_count,
                "recent_errors": self.metrics["additional_metrics"]["error_details"]
            }
            
            error_file = os.path.join(self.metrics_dir, "error_metrics.json")
            with open(error_file, 'w') as f:
                json.dump(error_metrics, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving error metrics: {str(e)}")

# Global instances for easy access
producer_metrics = ProducerMetricsCollector()
consumer_metrics = ConsumerMetricsCollector()
queue_metrics = QueueMetricsCollector()
error_metrics = ErrorMetricsCollector()

def start_all_collectors():
    """Start all metrics collectors"""
    producer_metrics.start_collecting()
    consumer_metrics.start_collecting()
    queue_metrics.start_collecting()
    error_metrics.start_collecting()

def stop_all_collectors():
    """Stop all metrics collectors"""
    producer_metrics.stop_collecting()
    consumer_metrics.stop_collecting()
    queue_metrics.stop_collecting()
    error_metrics.stop_collecting()

def record_system_error(component: str, error_type: str, message: str, severity: str = "error"):
    """
    Record an error in the system
    
    Args:
        component: Component where the error occurred
        error_type: Type of error
        message: Error message
        severity: Error severity
    """
    error_metrics.record_error(component, error_type, message, severity)
    
    # Log the error as well
    if severity == "critical":
        logger.critical(f"{component} - {error_type}: {message}")
    elif severity == "error":
        logger.error(f"{component} - {error_type}: {message}")
    elif severity == "warning":
        logger.warning(f"{component} - {error_type}: {message}")
    else:
        logger.info(f"{component} - {error_type}: {message}") 