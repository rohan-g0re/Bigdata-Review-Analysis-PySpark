"""
Script to verify data flow between batch and streaming modes in the Streamlit app
"""
import os
import time
import requests
import json
import pandas as pd
from datetime import datetime

def check_streamlit_status():
    """
    Check if Streamlit server is running and accessible
    
    Returns:
        bool: True if Streamlit is running
    """
    try:
        response = requests.get("http://localhost:8501/healthz")
        return response.status_code == 200
    except:
        return False

def main():
    """
    Verify the Streamlit app's data flow
    """
    print("=== STREAMLIT DATA FLOW VERIFICATION ===")
    print(f"Current time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Check if Streamlit is running
    if not check_streamlit_status():
        print("ERROR: Streamlit server is not running. Please start it with:")
        print("python -m streamlit run src/streamlit_app.py")
        return
    
    print("Streamlit server is running at http://localhost:8501")
    
    # Test both batch and streaming modes
    print("\n=== VERIFYING DATA FLOW ===")
    
    # Define test cases
    test_cases = [
        {
            "name": "Batch Mode (Parquet Files)",
            "expected_source": "Parquet files in data/all_reviews/cleaned_reviews",
            "expected_process": "Read data directly from Parquet files",
            "expected_mode": "batch"
        },
        {
            "name": "Streaming Mode (Mock Kafka)",
            "expected_source": "Kafka stream (mock mode)",
            "expected_process": "Consume messages from Kafka topic",
            "expected_mode": "streaming"
        }
    ]
    
    for test_case in test_cases:
        print(f"\nTesting: {test_case['name']}")
        print(f"Expected data source: {test_case['expected_source']}")
        print(f"Expected process: {test_case['expected_process']}")
        
        # In a real implementation, you would need to interact with Streamlit's session state
        # Since we can't directly do that from an external script, we'll just check files
        
        if test_case["expected_mode"] == "batch":
            # Check if Parquet files exist and can be read
            parquet_dir = "data/all_reviews/cleaned_reviews"
            if os.path.exists(parquet_dir) and os.path.isdir(parquet_dir):
                parquet_files = [f for f in os.listdir(parquet_dir) if f.endswith(".parquet")]
                if parquet_files:
                    print(f"✓ Found {len(parquet_files)} Parquet files in {parquet_dir}")
                    # Try reading a sample file
                    try:
                        sample_file = os.path.join(parquet_dir, parquet_files[0])
                        df = pd.read_parquet(sample_file)
                        print(f"✓ Successfully read sample file with {len(df)} records")
                        print(f"  Sample columns: {', '.join(df.columns[:5])}...")
                    except Exception as e:
                        print(f"✗ Error reading Parquet file: {str(e)}")
                else:
                    print(f"✗ No Parquet files found in {parquet_dir}")
            else:
                print(f"✗ Parquet directory {parquet_dir} not found")
        
        elif test_case["expected_mode"] == "streaming":
            # Verify the mock Kafka functionality
            try:
                from src.streaming.utils.mock_kafka import create_mock_producer, register_mock_consumer
                from src.streaming.config.settings import KAFKA_TOPIC
                
                # Try to create a mock producer and send a test message
                producer = create_mock_producer("localhost:9092")
                
                # Set up a message capture
                captured_messages = []
                def message_handler(topic, message):
                    captured_messages.append(message)
                
                # Register consumer
                register_mock_consumer(KAFKA_TOPIC, message_handler)
                
                # Send a test message
                test_message = {
                    "test_id": "verify_flow",
                    "timestamp": datetime.now().isoformat(),
                    "message": "Test message from verification script"
                }
                producer.send(KAFKA_TOPIC, test_message)
                
                # Wait a bit for message to be processed
                time.sleep(1)
                
                if captured_messages:
                    print(f"✓ Successfully sent and captured message through mock Kafka")
                    print(f"  Message content: {json.dumps(captured_messages[0])[:100]}...")
                else:
                    print(f"✗ Message was sent but not captured")
            except Exception as e:
                print(f"✗ Error testing mock Kafka: {str(e)}")
                
    print("\n=== VERIFICATION COMPLETE ===")

if __name__ == "__main__":
    main() 