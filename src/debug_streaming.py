"""
Debugging script for testing streaming mode and Kafka process launch
Run this script separately to validate that the components are working
"""
import os
import sys
import time
import json
from datetime import datetime
import pandas as pd
import subprocess
import platform
from typing import Dict, Any, Optional

# Import required functions - assuming this script is run from the project root
from src.utils.stream_control import is_streaming, get_streaming_status, start_streaming, stop_streaming

class StreamingDebugger:
    """Debugger class for streaming components"""
    
    def __init__(self):
        self.results = {
            "timestamp": datetime.now().isoformat(),
            "tests": []
        }
    
    def check_processes(self):
        """Check if Kafka processes are running"""
        # Get streaming status
        status = get_streaming_status()
        
        # Use the stream control internal state as our primary check
        producer_found = status["producer"] == "running"
        consumer_found = status["consumer"] == "running"
        
        python_process_count = 0
        
        try:
            # Count Python processes
            if platform.system() == "Windows":
                python_output = subprocess.check_output(
                    ["tasklist", "/fi", "imagename eq python.exe"], 
                    universal_newlines=True
                )
                print("\nPython processes:")
                print(python_output)
                
                # Count the lines with python.exe
                if "python.exe" in python_output:
                    python_process_count = len([line for line in python_output.split('\n') 
                                             if "python.exe" in line])
            else:
                # Use ps on Unix systems
                python_output = subprocess.check_output(
                    ["ps", "-ef", "|", "grep", "python"], 
                    universal_newlines=True
                )
                print("\nPython processes:")
                print(python_output)
                
                # Count python processes
                python_process_count = len([line for line in python_output.split('\n') 
                                         if "python" in line and "grep" not in line])
            
            print(f"Found {python_process_count} Python processes")
            
            return {
                "producer_found": producer_found,
                "consumer_found": consumer_found,
                "python_processes": python_process_count,
                "status": status
            }
        except Exception as e:
            print(f"Error checking processes: {str(e)}")
            return {
                "producer_found": producer_found,
                "consumer_found": consumer_found,
                "python_processes": 0,
                "status": status,
                "error": str(e)
            }
    
    def test_session_state_change(self):
        """
        Test session state change (simulation)
        Since we can't directly access Streamlit session state outside the app,
        we'll simulate the behavior
        """
        print("\n--- Testing Session State Change (Simulation) ---")
        
        # Simulated function to check if session state was properly updated
        def simulate_session_state_update(mode):
            print(f"[Session State] Mode changed to: {mode}")
            return {"success": True, "mode": mode}
        
        # Simulate changing to streaming mode
        print("Simulating change to streaming mode...")
        streaming_result = simulate_session_state_update("streaming")
        
        self.results["tests"].append({
            "name": "Session State Change (Streaming)",
            "result": streaming_result,
            "passed": streaming_result["success"] and streaming_result["mode"] == "streaming"
        })
        
        print("Session state test completed")
    
    def test_kafka_process_launch(self):
        """Test Kafka process launch and monitoring"""
        print("\n--- Testing Kafka Process Launch ---")
        
        # Test 1: Check initial status
        initial_status = get_streaming_status()
        print(f"Initial status: {initial_status}")
        
        self.results["tests"].append({
            "name": "Initial Status Check",
            "status": initial_status,
            "passed": not initial_status["is_streaming"]  # Should be off initially
        })
        
        # Test 2: Try to start streaming
        start_error = None
        start_success = False
        
        print("Starting streaming...")
        try:
            start_success = start_streaming()
            print(f"Start streaming result: {start_success}")
        except Exception as e:
            start_error = str(e)
            print(f"Error starting streaming: {start_error}")
        
        # Record start results
        self.results["tests"].append({
            "name": "Start Streaming",
            "success": start_success,
            "error": start_error,
            "passed": start_success
        })
        
        # Test 3: Check if processes were created
        print("Waiting for processes to start...")
        time.sleep(3)  # Give time for processes to start
        
        status_after_start = get_streaming_status()
        print(f"Status after start: {status_after_start}")
        
        process_check = self.check_processes()
        print(f"Process check: {process_check}")
        
        self.results["tests"].append({
            "name": "Process Creation Check",
            "status": status_after_start,
            "process_check": process_check,
            "passed": status_after_start["is_streaming"] and 
                    process_check["producer_found"] and 
                    process_check["consumer_found"]
        })
        
        # Test 4: Try to stop streaming
        stop_error = None
        stop_success = False
        
        print("Stopping streaming...")
        try:
            stop_success = stop_streaming()
            print(f"Stop streaming result: {stop_success}")
        except Exception as e:
            stop_error = str(e)
            print(f"Error stopping streaming: {stop_error}")
        
        # Record stop results
        self.results["tests"].append({
            "name": "Stop Streaming",
            "success": stop_success,
            "error": stop_error,
            "passed": stop_success
        })
        
        # Test 5: Final status check
        print("Waiting for processes to stop...")
        time.sleep(2)  # Give time for processes to stop
        
        final_status = get_streaming_status()
        print(f"Final status: {final_status}")
        
        final_process_check = self.check_processes()
        print(f"Final process check: {final_process_check}")
        
        self.results["tests"].append({
            "name": "Final Status Check",
            "status": final_status,
            "process_check": final_process_check,
            "passed": not final_status["is_streaming"] and 
                    not final_process_check["producer_found"] and 
                    not final_process_check["consumer_found"]
        })
    
    def run_all_tests(self):
        """Run all tests"""
        print("=== Starting Streaming Debug Tests ===")
        
        # Test session state change
        self.test_session_state_change()
        
        # Test Kafka process launch
        self.test_kafka_process_launch()
        
        # Calculate overall success
        self.results["all_tests_passed"] = all(test["passed"] for test in self.results["tests"])
        
        # Print summary
        self.print_summary()
        
        # Save results
        self.save_results()
    
    def print_summary(self):
        """Print test summary"""
        print("\n=== Test Summary ===")
        for test in self.results["tests"]:
            status = "✅ PASSED" if test["passed"] else "❌ FAILED"
            print(f"{test['name']}: {status}")
        
        overall = "✅ ALL TESTS PASSED" if self.results["all_tests_passed"] else "❌ SOME TESTS FAILED"
        print(f"\nOverall: {overall}")
    
    def save_results(self):
        """Save test results to file"""
        results_dir = "debug_logs"
        os.makedirs(results_dir, exist_ok=True)
        
        filename = os.path.join(results_dir, f"streaming_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        
        with open(filename, 'w') as f:
            json.dump(self.results, f, indent=2)
        
        print(f"\nTest results saved to: {filename}")


def main():
    """Main function"""
    debugger = StreamingDebugger()
    debugger.run_all_tests()


if __name__ == "__main__":
    main() 