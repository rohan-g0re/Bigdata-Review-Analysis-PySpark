"""
Unified system runner for the Steam reviews streaming pipeline

This script can run all components of the system either independently or together.
"""
import os
import sys
import time
import signal
import argparse
import threading
import subprocess
import logging
from typing import Dict, Any, List, Optional, Tuple

from src.streaming.utils.logging_config import setup_logger
from src.streaming.producer.kafka_producer import create_and_start_producer
from src.streaming.consumer.kafka_consumer import create_and_start_consumer
from src.streaming.monitoring.metrics_collector import start_all_collectors, stop_all_collectors
from src.streaming.config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    KAFKA_CONSUMER_GROUP,
    PARQUET_DIR,
    STREAM_RESULTS_DIR,
    BATCH_SIZE,
    THROTTLE_RATE,
    USE_MOCK_KAFKA
)

# Set up logging
logger = setup_logger("run_system", "run_system.log")

# Flag to track if the system is running
running = True

def handle_interrupt(signum, frame):
    """Handle interrupt signal"""
    global running
    logger.info("Received interrupt signal. Shutting down...")
    running = False

def run_producer(
    bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
    topic: str = KAFKA_TOPIC,
    parquet_dir: str = PARQUET_DIR,
    batch_size: int = BATCH_SIZE,
    throttle_rate: float = THROTTLE_RATE,
    max_records: Optional[int] = None,
    max_files: Optional[int] = None
) -> bool:
    """
    Run the producer component
    
    Args:
        bootstrap_servers: Kafka bootstrap servers
        topic: Kafka topic
        parquet_dir: Path to Parquet files
        batch_size: Batch size for reading
        throttle_rate: Throttle rate for sending
        max_records: Maximum number of records to send
        max_files: Maximum number of files to process
        
    Returns:
        bool: True if producer ran successfully
    """
    logger.info("Starting producer component")
    
    # Create and run the producer
    result = create_and_start_producer(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        parquet_dir=parquet_dir,
        batch_size=batch_size,
        throttle_rate=throttle_rate,
        max_records=max_records,
        max_files=max_files
    )
    
    if result:
        logger.info("Producer completed successfully")
    else:
        logger.error("Producer failed")
    
    return result

def run_consumer(
    bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
    topic: str = KAFKA_TOPIC,
    group_id: str = KAFKA_CONSUMER_GROUP,
    results_dir: str = STREAM_RESULTS_DIR,
    max_messages: Optional[int] = None
) -> bool:
    """
    Run the consumer component
    
    Args:
        bootstrap_servers: Kafka bootstrap servers
        topic: Kafka topic
        group_id: Consumer group ID
        results_dir: Directory to save results
        max_messages: Maximum number of messages to consume
        
    Returns:
        bool: True if consumer ran successfully
    """
    logger.info("Starting consumer component")
    
    # Create and run the consumer
    result = create_and_start_consumer(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        group_id=group_id,
        results_dir=results_dir,
        max_messages=max_messages
    )
    
    if result:
        logger.info("Consumer completed successfully")
    else:
        logger.error("Consumer failed")
    
    return result

def run_dashboard(port: int = 8501) -> subprocess.Popen:
    """
    Run the Streamlit dashboard
    
    Args:
        port: Port for the dashboard
        
    Returns:
        subprocess.Popen: Process object
    """
    logger.info(f"Starting dashboard on port {port}")
    
    try:
        # Get the path to the Streamlit app
        app_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
            "streamlit_app.py"
        )
        
        # Build the command to run the dashboard
        command = [
            "streamlit", "run", app_path,
            "--server.port", str(port),
            "--server.address", "0.0.0.0"
        ]
        
        # Run the command
        process = subprocess.Popen(command)
        logger.info(f"Dashboard started at http://localhost:{port}")
        
        return process
    except Exception as e:
        logger.error(f"Error starting dashboard: {str(e)}")
        return None

def run_monitor(port: int = 8502) -> subprocess.Popen:
    """
    Run the monitoring dashboard
    
    Args:
        port: Port for the monitoring dashboard
        
    Returns:
        subprocess.Popen: Process object
    """
    logger.info(f"Starting monitoring dashboard on port {port}")
    
    try:
        # Get the path to the monitor dashboard script
        monitor_script_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "monitoring",
            "monitor_dashboard.py"
        )
        
        # Build the command to run the monitoring dashboard
        command = [
            "streamlit", "run", monitor_script_path,
            "--server.port", str(port),
            "--server.address", "0.0.0.0"
        ]
        
        # Run the command
        process = subprocess.Popen(command)
        logger.info(f"Monitoring dashboard started at http://localhost:{port}")
        
        return process
    except Exception as e:
        logger.error(f"Error starting monitoring dashboard: {str(e)}")
        return None

def run_all(
    bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
    topic: str = KAFKA_TOPIC,
    group_id: str = KAFKA_CONSUMER_GROUP,
    parquet_dir: str = PARQUET_DIR,
    results_dir: str = STREAM_RESULTS_DIR,
    batch_size: int = BATCH_SIZE,
    throttle_rate: float = THROTTLE_RATE,
    max_records: Optional[int] = None,
    max_files: Optional[int] = None,
    max_messages: Optional[int] = None,
    dashboard: bool = False,
    dashboard_port: int = 8501,
    monitor: bool = False,
    monitor_port: int = 8502
) -> bool:
    """
    Run all components together
    
    Args:
        bootstrap_servers: Kafka bootstrap servers
        topic: Kafka topic
        group_id: Consumer group ID
        parquet_dir: Path to Parquet files
        results_dir: Directory to save results
        batch_size: Batch size for reading
        throttle_rate: Throttle rate for sending
        max_records: Maximum number of records to send
        max_files: Maximum number of files to process
        max_messages: Maximum number of messages to consume
        dashboard: Whether to run the Streamlit dashboard
        dashboard_port: Port for the dashboard
        monitor: Whether to run the monitoring dashboard
        monitor_port: Port for the monitoring dashboard
        
    Returns:
        bool: True if all components ran successfully
    """
    global running
    
    # Set up signal handling in the main thread
    signal.signal(signal.SIGINT, handle_interrupt)
    
    # Create processes list
    processes = []
    
    # Start metrics collectors
    start_all_collectors()
    logger.info("Started metrics collection")
    
    try:
        # Start monitoring dashboard if requested
        if monitor:
            monitor_process = run_monitor(port=monitor_port)
            if monitor_process:
                processes.append(("monitor", monitor_process))
        
        # Start dashboard if requested
        if dashboard:
            dashboard_process = run_dashboard(port=dashboard_port)
            if dashboard_process:
                processes.append(("dashboard", dashboard_process))
        
        # Start consumer in a thread
        def run_consumer_thread():
            run_consumer(
                bootstrap_servers=bootstrap_servers,
                topic=topic,
                group_id=group_id,
                results_dir=results_dir,
                max_messages=max_messages
            )
        
        consumer_thread = threading.Thread(target=run_consumer_thread)
        consumer_thread.daemon = True
        consumer_thread.start()
        logger.info("Started consumer thread")
        
        # Give the consumer a moment to initialize
        time.sleep(2)
        
        # Run producer (this will block until completion)
        producer_result = run_producer(
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            parquet_dir=parquet_dir,
            batch_size=batch_size,
            throttle_rate=throttle_rate,
            max_records=max_records,
            max_files=max_files
        )
        
        # Wait for consumer thread to finish (with timeout)
        consumer_timeout = 30  # seconds
        consumer_thread.join(timeout=consumer_timeout)
        
        # Keep system running if dashboards are active and no limits were set
        if (dashboard or monitor) and not max_records and not max_files and not max_messages:
            logger.info("Producer finished but keeping system running for dashboards...")
            
            while running:
                time.sleep(1)
        
        # Success if producer succeeded
        return producer_result
        
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
        running = False
        return False
    except Exception as e:
        logger.error(f"Error running system: {str(e)}")
        running = False
        return False
    finally:
        # Stop metrics collection
        stop_all_collectors()
        logger.info("Stopped metrics collection")
        
        # Clean up processes
        for name, process in processes:
            try:
                logger.info(f"Terminating {name} process")
                process.terminate()
                process.wait(timeout=5)
            except Exception as e:
                logger.error(f"Error terminating {name} process: {str(e)}")
                try:
                    process.kill()
                except:
                    pass
        
        logger.info("All components shut down")

def main():
    """Main function for the system runner"""
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Run the Steam reviews streaming system")
    
    # Component selection
    parser.add_argument(
        "--component", 
        choices=["producer", "consumer", "dashboard", "monitor", "all"],
        default="all", 
        help="Component to run (default: all)"
    )
    
    # Kafka configuration
    parser.add_argument(
        "--bootstrap-servers", 
        default=KAFKA_BOOTSTRAP_SERVERS,
        help=f"Kafka bootstrap servers (default: {KAFKA_BOOTSTRAP_SERVERS})"
    )
    parser.add_argument(
        "--topic", 
        default=KAFKA_TOPIC,
        help=f"Kafka topic (default: {KAFKA_TOPIC})"
    )
    parser.add_argument(
        "--group-id", 
        default=KAFKA_CONSUMER_GROUP,
        help=f"Kafka consumer group ID (default: {KAFKA_CONSUMER_GROUP})"
    )
    
    # Data and results directories
    parser.add_argument(
        "--parquet-dir", 
        default=PARQUET_DIR,
        help=f"Directory containing Parquet files (default: {PARQUET_DIR})"
    )
    parser.add_argument(
        "--results-dir", 
        default=STREAM_RESULTS_DIR,
        help=f"Directory to save results (default: {STREAM_RESULTS_DIR})"
    )
    
    # Performance configuration
    parser.add_argument(
        "--batch-size", 
        type=int, 
        default=BATCH_SIZE,
        help=f"Batch size for reading (default: {BATCH_SIZE})"
    )
    parser.add_argument(
        "--throttle-rate", 
        type=float, 
        default=THROTTLE_RATE,
        help=f"Throttle rate for sending (default: {THROTTLE_RATE})"
    )
    
    # Testing limits
    parser.add_argument(
        "--max-records", 
        type=int, 
        default=None,
        help="Maximum number of records to send (default: no limit)"
    )
    parser.add_argument(
        "--max-files", 
        type=int, 
        default=None,
        help="Maximum number of files to process (default: no limit)"
    )
    parser.add_argument(
        "--max-messages", 
        type=int, 
        default=None,
        help="Maximum number of messages to consume (default: no limit)"
    )
    
    # Dashboard options
    parser.add_argument(
        "--dashboard", 
        action="store_true",
        help="Run the Streamlit dashboard"
    )
    parser.add_argument(
        "--dashboard-port", 
        type=int, 
        default=8501,
        help="Port for the Streamlit dashboard (default: 8501)"
    )
    
    # Monitoring options
    parser.add_argument(
        "--monitor", 
        action="store_true",
        help="Run the monitoring dashboard"
    )
    parser.add_argument(
        "--monitor-port", 
        type=int, 
        default=8502,
        help="Port for the monitoring dashboard (default: 8502)"
    )
    
    args = parser.parse_args()
    
    # Run the selected component
    if args.component == "producer":
        run_producer(
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
            parquet_dir=args.parquet_dir,
            batch_size=args.batch_size,
            throttle_rate=args.throttle_rate,
            max_records=args.max_records,
            max_files=args.max_files
        )
    elif args.component == "consumer":
        run_consumer(
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
            group_id=args.group_id,
            results_dir=args.results_dir,
            max_messages=args.max_messages
        )
    elif args.component == "dashboard":
        dashboard_process = run_dashboard(port=args.dashboard_port)
        if dashboard_process:
            try:
                dashboard_process.wait()
            except KeyboardInterrupt:
                dashboard_process.terminate()
                dashboard_process.wait()
    elif args.component == "monitor":
        monitor_process = run_monitor(port=args.monitor_port)
        if monitor_process:
            try:
                monitor_process.wait()
            except KeyboardInterrupt:
                monitor_process.terminate()
                monitor_process.wait()
    else:  # "all"
        run_all(
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
            group_id=args.group_id,
            parquet_dir=args.parquet_dir,
            results_dir=args.results_dir,
            batch_size=args.batch_size,
            throttle_rate=args.throttle_rate,
            max_records=args.max_records,
            max_files=args.max_files,
            max_messages=args.max_messages,
            dashboard=args.dashboard,
            dashboard_port=args.dashboard_port,
            monitor=args.monitor,
            monitor_port=args.monitor_port
        )

if __name__ == "__main__":
    main() 