"""
Utility module to control Kafka streaming from the Streamlit dashboard
"""
import subprocess
import threading
import time
import os
import signal
import logging
from typing import Dict, Any, Optional

# Import config settings
from src.streaming.config.settings import USE_MOCK_KAFKA

logger = logging.getLogger(__name__)

# Global variables to track process state
_producer_process = None
_consumer_process = None
_is_streaming = False
_using_mock = USE_MOCK_KAFKA

def start_streaming():
    """
    Start the Kafka producer and consumer processes
    
    Returns:
        bool: True if started successfully
    """
    global _producer_process, _consumer_process, _is_streaming, _using_mock
    
    if _is_streaming:
        logger.info("Streaming is already running")
        return True
    
    # Check if we're using mock mode
    if _using_mock:
        logger.info("Using mock Kafka mode - no real processes will be started")
        _is_streaming = True
        
        # Start a mock monitoring thread
        threading.Thread(target=_mock_monitor, daemon=True).start()
        
        return True
    
    try:
        # Use the DETACHED_PROCESS flag on Windows to keep processes running
        # even if the parent process terminates
        creation_flags = 0
        if os.name == 'nt':  # Windows
            creation_flags = subprocess.DETACHED_PROCESS
        
        # Start the producer in a separate process with detached flag
        _producer_process = subprocess.Popen(
            ["python", "-m", "src.streaming.run_system", "--component", "producer"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            creationflags=creation_flags
        )
        logger.info("Started producer process")
        
        # Start the consumer in a separate process with detached flag
        _consumer_process = subprocess.Popen(
            ["python", "-m", "src.streaming.run_system", "--component", "consumer"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            creationflags=creation_flags
        )
        logger.info("Started consumer process")
        
        # Set the streaming flag
        _is_streaming = True
        
        # Start monitoring thread
        threading.Thread(target=_monitor_processes, daemon=True).start()
        
        return True
    except Exception as e:
        logger.error(f"Error starting streaming: {str(e)}")
        stop_streaming()
        return False

def stop_streaming():
    """
    Stop the Kafka producer and consumer processes
    
    Returns:
        bool: True if stopped successfully
    """
    global _producer_process, _consumer_process, _is_streaming, _using_mock
    
    if not _is_streaming:
        logger.info("Streaming is not running")
        return True
    
    # Check if we're using mock mode
    if _using_mock:
        logger.info("Stopping mock Kafka streams")
        _is_streaming = False
        return True
    
    try:
        # Stop the producer
        if _producer_process:
            _producer_process.terminate()
            try:
                _producer_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                _producer_process.kill()
            logger.info("Stopped producer process")
        
        # Stop the consumer
        if _consumer_process:
            _consumer_process.terminate()
            try:
                _consumer_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                _consumer_process.kill()
            logger.info("Stopped consumer process")
        
        # Reset the processes
        _producer_process = None
        _consumer_process = None
        
        # Reset the streaming flag
        _is_streaming = False
        
        return True
    except Exception as e:
        logger.error(f"Error stopping streaming: {str(e)}")
        return False

def is_streaming():
    """
    Check if streaming is currently active
    
    Returns:
        bool: True if streaming is active
    """
    global _is_streaming
    return _is_streaming

def get_streaming_status():
    """
    Get the status of the streaming processes
    
    Returns:
        Dict: Status information
    """
    global _using_mock, _is_streaming
    
    if _using_mock:
        # In mock mode, base the status purely on the _is_streaming flag
        return {
            "is_streaming": _is_streaming,
            "producer": "running" if _is_streaming else "stopped",
            "consumer": "running" if _is_streaming else "stopped",
            "mock_mode": True
        }
    
    # Normal mode - check actual processes
    producer_status = "running" if _producer_process and _producer_process.poll() is None else "stopped"
    consumer_status = "running" if _consumer_process and _consumer_process.poll() is None else "stopped"
    
    return {
        "is_streaming": _is_streaming,
        "producer": producer_status,
        "consumer": consumer_status,
        "mock_mode": False
    }

def _mock_monitor():
    """
    Mock monitoring function that just keeps track of the _is_streaming flag
    """
    global _is_streaming
    
    logger.info("Starting mock Kafka monitoring")
    
    while _is_streaming:
        time.sleep(1)
    
    logger.info("Mock Kafka monitoring stopped")

def _monitor_processes():
    """
    Monitor the producer and consumer processes
    and update the status accordingly
    """
    global _producer_process, _consumer_process, _is_streaming
    
    while _is_streaming:
        # Check if the processes are still running
        producer_running = _producer_process and _producer_process.poll() is None
        consumer_running = _consumer_process and _consumer_process.poll() is None
        
        # If both processes have stopped, update the status
        if not producer_running and not consumer_running:
            logger.info("Both producer and consumer have stopped")
            _is_streaming = False
            break
        
        # Sleep for a bit
        time.sleep(1) 