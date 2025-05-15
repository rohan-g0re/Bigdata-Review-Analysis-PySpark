"""
Helper script to run the Streamlit app with debugging enabled
"""
import subprocess
import os
import time
import sys

def main():
    """Run the Streamlit app with debug logging"""
    # Set environment variable to enable detailed logging
    os.environ["DEBUG_STREAMING"] = "1"
    
    print("Starting Streamlit app with debugging enabled...")
    print("1. Access the Debug tab in the app")
    print("2. Use the Debug tab to check process and session state")
    print("3. Press Ctrl+C in this terminal to stop the app")
    
    try:
        # Run the Streamlit app using python -m streamlit instead of direct command
        subprocess.run(["python", "-m", "streamlit", "run", "src/streamlit_app.py"])
    except KeyboardInterrupt:
        print("\nStreamlit app stopped.")
    
    print("Checking for any running Kafka processes...")
    
    # Import after running to avoid conflicts with Streamlit
    from src.utils.debug_utils import stream_debugger
    
    # Check for any leftover processes
    process_check = stream_debugger.check_processes()
    print(f"Process check: {process_check}")
    
    # Try to stop streaming just in case
    from src.utils.stream_control import stop_streaming
    stop_streaming()
    print("Cleanup complete.")

if __name__ == "__main__":
    main() 