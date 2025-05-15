"""
Special debugging script for Kafka processes with mock mode enabled
"""
import os
import time
import sys
import json
from datetime import datetime

# Ensure mock Kafka is enabled
os.environ["USE_MOCK_KAFKA"] = "True"

# Import after setting environment variable
from src.utils.stream_control import start_streaming, stop_streaming, get_streaming_status
from src.streaming.config.settings import USE_MOCK_KAFKA

def test_mock_kafka():
    """Test if mock Kafka is working correctly"""
    print("=== Testing Mock Kafka ===")
    print(f"USE_MOCK_KAFKA setting: {USE_MOCK_KAFKA}")
    
    # Initial status
    print("\nInitial status:")
    initial_status = get_streaming_status()
    print(json.dumps(initial_status, indent=2))
    
    # Start streaming
    print("\nStarting streaming...")
    start_result = start_streaming()
    print(f"Start result: {start_result}")
    
    # Check status after start
    time.sleep(1)
    print("\nStatus after start:")
    after_start_status = get_streaming_status()
    print(json.dumps(after_start_status, indent=2))
    
    # Wait a bit to see if status changes
    print("\nWaiting 5 seconds to see if status updates...")
    for i in range(5):
        time.sleep(1)
        print(f"Status after {i+1} seconds: {get_streaming_status()}")
    
    # Final status check
    final_status = get_streaming_status()
    print("\nFinal status:")
    print(json.dumps(final_status, indent=2))
    
    # Stop streaming
    print("\nStopping streaming...")
    stop_result = stop_streaming()
    print(f"Stop result: {stop_result}")
    
    # Status after stop
    time.sleep(1)
    after_stop_status = get_streaming_status()
    print("\nStatus after stop:")
    print(json.dumps(after_stop_status, indent=2))
    
    # Summary
    print("\n=== Mock Kafka Test Summary ===")
    print(f"Mock Kafka enabled: {USE_MOCK_KAFKA}")
    print(f"Start streaming returned: {start_result}")
    print(f"Stop streaming returned: {stop_result}")
    print(f"Streaming was active: {final_status['is_streaming']}")
    print(f"Producer status: {final_status['producer']}")
    print(f"Consumer status: {final_status['consumer']}")
    
    success = (
        start_result and 
        stop_result and
        final_status['is_streaming'] and
        final_status['producer'] == 'running' and
        final_status['consumer'] == 'running'
    )
    
    if success:
        print("\n✅ Mock Kafka is working correctly!")
    else:
        print("\n❌ Mock Kafka is NOT working correctly.")
        print("Possible issues:")
        print("1. The mock implementation in stream_control.py isn't updating status flags")
        print("2. Dependencies might be missing")
        print("3. The mock Kafka environment isn't properly configured")

if __name__ == "__main__":
    test_mock_kafka() 