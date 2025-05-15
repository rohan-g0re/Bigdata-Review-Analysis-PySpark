"""
Debug script to verify streaming mode functionality
"""
import os
import time
import streamlit as st
from datetime import datetime

# Set mock Kafka mode
os.environ["USE_MOCK_KAFKA"] = "True"

# Import necessary modules
from src.utils.stream_control import is_streaming, get_streaming_status, start_streaming, stop_streaming
from src.utils.debug_utils import stream_debugger, test_streaming_toggle, test_kafka_process_launch

def main():
    """Main debug function to run tests for streaming mode"""
    print("==== DEBUG SCRIPT FOR STREAMING MODE ====")
    print(f"Current time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Initialize session state for testing
    if "data_mode" not in st.session_state:
        st.session_state["data_mode"] = "batch"
        print("Initialized session state with data_mode = 'batch'")
    
    # Show current status
    print("\n=== CURRENT STATUS ===")
    current_mode = st.session_state.get("data_mode", "Not set")
    print(f"Current data mode: {current_mode}")
    
    status = get_streaming_status()
    print(f"Is streaming: {status['is_streaming']}")
    print(f"Producer status: {status['producer']}")
    print(f"Consumer status: {status['consumer']}")
    print(f"Mock mode: {status.get('mock_mode', False)}")
    
    # Run streaming toggle test
    print("\n=== RUNNING STREAMING TOGGLE TEST ===")
    toggle_results = test_streaming_toggle()
    print(f"All tests passed: {toggle_results['all_tests_passed']}")
    
    for test in toggle_results['tests']:
        print(f"- {test['name']}: {'✓' if test['passed'] else '✗'}")
    
    # Final status check
    print("\n=== FINAL STATUS ===")
    st.session_state["data_mode"] = "streaming"
    print(f"Session state data_mode: {st.session_state['data_mode']}")
    
    status = get_streaming_status()
    print(f"Is streaming: {status['is_streaming']}")
    print(f"Producer status: {status['producer']}")
    print(f"Consumer status: {status['consumer']}")
    print(f"Mock mode: {status.get('mock_mode', False)}")
    
    # Test a manual start/stop cycle
    print("\n=== TESTING MANUAL START/STOP ===")
    print("Starting streaming...")
    start_streaming()
    
    # Get status after start
    status = get_streaming_status()
    print(f"Is streaming: {status['is_streaming']}")
    print(f"Producer status: {status['producer']}")
    print(f"Consumer status: {status['consumer']}")
    
    # Wait a bit
    print("Waiting 2 seconds...")
    time.sleep(2)
    
    # Stop streaming
    print("Stopping streaming...")
    stop_streaming()
    
    # Get status after stop
    status = get_streaming_status()
    print(f"Is streaming: {status['is_streaming']}")
    print(f"Producer status: {status['producer']}")
    print(f"Consumer status: {status['consumer']}")
    
    print("\n==== DEBUG COMPLETE ====")

if __name__ == "__main__":
    main() 