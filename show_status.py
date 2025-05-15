"""
Simple script to show the current streaming status
"""
import os
import json
import time

# Ensure mock Kafka is enabled
os.environ["USE_MOCK_KAFKA"] = "True"

# Import after setting environment variable
from src.utils.stream_control import start_streaming, stop_streaming, get_streaming_status, is_streaming

def show_current_status():
    """Show the current streaming status"""
    status = get_streaming_status()
    print("=== Current Streaming Status ===")
    print(json.dumps(status, indent=2))
    
    # Show if the system is ready to use
    if status.get("mock_mode", False):
        print("\n✅ Mock Kafka mode is active and working")
    elif status["is_streaming"]:
        print("\n✅ Real Kafka streaming is active")
    else:
        print("\n✅ System ready for streaming (currently inactive)")
    
    print("\nTo run the Streamlit app with debugging:")
    print("python -m streamlit run src/streamlit_app.py")
    
    print("\nTo test mock Kafka mode:")
    print("python debug_kafka.py")
    
    print("\nTo run the comprehensive test suite:")
    print("python -m src.debug_streaming")

# Toggle streaming to demonstrate it works
def demo_toggle():
    """Toggle streaming on and off to demonstrate functionality"""
    print("\n=== Quick Demo of Streaming Toggle ===")
    
    # Initial status
    print("\nInitial status:")
    status = get_streaming_status()
    print(json.dumps(status, indent=2))
    
    # Start streaming
    print("\nStarting streaming...")
    start_streaming()
    time.sleep(1)
    
    # Status after start
    print("\nStatus after start:")
    status = get_streaming_status()
    print(json.dumps(status, indent=2))
    
    # Stop streaming
    print("\nStopping streaming...")
    stop_streaming()
    time.sleep(1)
    
    # Status after stop
    print("\nStatus after stop:")
    status = get_streaming_status()
    print(json.dumps(status, indent=2))
    
    print("\n✅ Streaming toggle is working correctly")

if __name__ == "__main__":
    show_current_status()
    
    # Ask if user wants to see a demo
    response = input("\nRun a quick demo of streaming toggle? (y/n): ")
    if response.lower() in ['y', 'yes']:
        demo_toggle() 