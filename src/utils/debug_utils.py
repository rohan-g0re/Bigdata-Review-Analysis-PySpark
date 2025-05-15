"""
Debug utilities for the Streamlit dashboard
Used to verify the proper functioning of streaming mode and Kafka processes
"""
import streamlit as st
import time
import os
import json
from datetime import datetime
import subprocess
import pandas as pd
import platform
from typing import Dict, Any, Optional

# Import stream control functions to check status
from src.utils.stream_control import is_streaming, get_streaming_status, start_streaming, stop_streaming


class StreamDebugger:
    """Class to handle debugging of streaming processes"""
    
    def __init__(self, log_path: str = "debug_logs"):
        """Initialize the debugger"""
        self.log_path = log_path
        self.session_log_file = os.path.join(log_path, "session_state_log.json")
        self.process_log_file = os.path.join(log_path, "process_log.json")
        
        # Create log directory if it doesn't exist
        os.makedirs(log_path, exist_ok=True)
    
    def log_session_state_change(self, prev_mode: str, new_mode: str) -> None:
        """
        Log a change in session state mode
        
        Args:
            prev_mode: Previous data mode
            new_mode: New data mode
        """
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "event": "session_state_change",
            "previous_mode": prev_mode,
            "new_mode": new_mode
        }
        
        self._append_to_log(self.session_log_file, log_entry)
        
    def log_process_start_attempt(self, success: bool, error_msg: Optional[str] = None) -> None:
        """
        Log a Kafka process start attempt
        
        Args:
            success: Whether the process start was successful
            error_msg: Error message if any
        """
        status = get_streaming_status()
        
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "event": "process_start_attempt",
            "success": success,
            "is_streaming": status["is_streaming"],
            "producer_status": status["producer"],
            "consumer_status": status["consumer"]
        }
        
        if error_msg:
            log_entry["error"] = error_msg
            
        self._append_to_log(self.process_log_file, log_entry)
    
    def get_session_state_logs(self) -> pd.DataFrame:
        """
        Get session state logs as a DataFrame
        
        Returns:
            DataFrame containing session state logs
        """
        logs = self._read_log(self.session_log_file)
        if logs:
            return pd.DataFrame(logs)
        return pd.DataFrame(columns=["timestamp", "event", "previous_mode", "new_mode"])
    
    def get_process_logs(self) -> pd.DataFrame:
        """
        Get process logs as a DataFrame
        
        Returns:
            DataFrame containing process logs
        """
        logs = self._read_log(self.process_log_file)
        if logs:
            return pd.DataFrame(logs)
        return pd.DataFrame(columns=["timestamp", "event", "success", "is_streaming", 
                                    "producer_status", "consumer_status", "error"])
    
    def check_processes(self) -> Dict[str, Any]:
        """
        Check if Kafka processes are running
        
        Returns:
            Dict with process status
        """
        # Get process status from stream_control
        status = get_streaming_status()
        
        # Use the stream control internal state as our primary check
        producer_found = status["producer"] == "running"
        consumer_found = status["consumer"] == "running"
        is_mock = status.get("mock_mode", False)
        
        python_process_count = 0
        
        # If we're in mock mode, just return the mock status
        if is_mock:
            return {
                "stream_control_status": status,
                "process_check": {
                    "producer_found": producer_found,
                    "consumer_found": consumer_found
                },
                "python_processes": 0,
                "mock_mode": True
            }
        
        # Only try to check actual processes if not in mock mode
        try:
            # Count Python processes
            if platform.system() == "Windows":
                try:
                    python_output = subprocess.check_output(
                        ["tasklist", "/fi", "imagename eq python.exe"], 
                        universal_newlines=True
                    )
                    # Count the lines (subtract 2 for header)
                    if "python.exe" in python_output:
                        python_process_count = len([line for line in python_output.split('\n') 
                                                 if "python.exe" in line])
                except Exception as e:
                    # On Windows, if this fails, just use the status report
                    return {
                        "stream_control_status": status,
                        "process_check": {
                            "producer_found": producer_found,
                            "consumer_found": consumer_found
                        },
                        "python_processes": 0
                    }
            else:
                # Use ps on Unix systems
                python_output = subprocess.check_output(
                    ["ps", "-ef", "|", "grep", "python"], 
                    universal_newlines=True
                )
                # Count python processes
                python_process_count = len([line for line in python_output.split('\n') 
                                         if "python" in line and "grep" not in line])
        except Exception as e:
            # If process checking fails, just use the status
            return {
                "stream_control_status": status,
                "process_check": {
                    "producer_found": producer_found,
                    "consumer_found": consumer_found
                },
                "python_processes": 0
            }
            
        return {
            "stream_control_status": status,
            "process_check": {
                "producer_found": producer_found,
                "consumer_found": consumer_found
            },
            "python_processes": python_process_count
        }
        
    def _append_to_log(self, log_file: str, log_entry: Dict[str, Any]) -> None:
        """Append an entry to a log file"""
        try:
            # Read existing logs
            logs = self._read_log(log_file)
            
            # Append new entry
            logs.append(log_entry)
            
            # Write back to file
            with open(log_file, 'w') as f:
                json.dump(logs, f, indent=2)
        except Exception as e:
            st.error(f"Error writing to log file: {str(e)}")
    
    def _read_log(self, log_file: str) -> list:
        """Read logs from a log file"""
        try:
            if os.path.exists(log_file):
                with open(log_file, 'r') as f:
                    return json.load(f)
            return []
        except Exception as e:
            st.error(f"Error reading log file: {str(e)}")
            return []


# Create a singleton instance for use throughout the app
stream_debugger = StreamDebugger()


def test_kafka_process_launch():
    """
    Test the Kafka process launch and return detailed information
    
    Returns:
        Dict with test results
    """
    results = {
        "timestamp": datetime.now().isoformat(),
        "tests": []
    }
    
    # Test 1: Check initial status
    initial_status = get_streaming_status()
    results["tests"].append({
        "name": "Initial Status Check",
        "status": initial_status,
        "passed": not initial_status["is_streaming"]  # Should be off initially
    })
    
    # Test 2: Try to start streaming
    start_error = None
    start_success = False
    try:
        start_success = start_streaming()
    except Exception as e:
        start_error = str(e)
    
    # Record start results
    results["tests"].append({
        "name": "Start Streaming",
        "success": start_success,
        "error": start_error,
        "passed": start_success
    })
    
    # Test 3: Check if processes were created
    time.sleep(2)  # Give time for processes to start
    status_after_start = get_streaming_status()
    process_check = stream_debugger.check_processes()
    
    results["tests"].append({
        "name": "Process Creation Check",
        "status": status_after_start,
        "process_check": process_check,
        "passed": status_after_start["is_streaming"] and 
                process_check["process_check"]["producer_found"] and 
                process_check["process_check"]["consumer_found"]
    })
    
    # Test 4: Try to stop streaming
    stop_error = None
    stop_success = False
    try:
        stop_success = stop_streaming()
    except Exception as e:
        stop_error = str(e)
    
    # Record stop results
    results["tests"].append({
        "name": "Stop Streaming",
        "success": stop_success,
        "error": stop_error,
        "passed": stop_success
    })
    
    # Test 5: Final status check
    time.sleep(2)  # Give time for processes to stop
    final_status = get_streaming_status()
    final_process_check = stream_debugger.check_processes()
    
    results["tests"].append({
        "name": "Final Status Check",
        "status": final_status,
        "process_check": final_process_check,
        "passed": not final_status["is_streaming"] and 
                not final_process_check["process_check"]["producer_found"] and 
                not final_process_check["process_check"]["consumer_found"]
    })
    
    # Calculate overall success
    results["all_tests_passed"] = all(test["passed"] for test in results["tests"])
    
    return results


def test_streaming_toggle() -> Dict[str, Any]:
    """
    Test toggling between batch and streaming modes
    
    Returns:
        Dict with test results
    """
    results = {
        "timestamp": datetime.now().isoformat(),
        "tests": []
    }
    
    # Import streamlit dependencies for session state simulation
    import streamlit as st
    
    # Test 1: Initialize session state
    if "data_mode" not in st.session_state:
        st.session_state["data_mode"] = "batch"
    
    initial_mode = st.session_state["data_mode"]
    results["tests"].append({
        "name": "Initial Mode Check",
        "mode": initial_mode,
        "passed": initial_mode == "batch"  # Should start in batch mode
    })
    
    # Test 2: Toggle to streaming mode
    prev_mode = st.session_state["data_mode"]
    st.session_state["data_mode"] = "streaming"
    new_mode = st.session_state["data_mode"]
    
    # Log the change
    stream_debugger.log_session_state_change(prev_mode, new_mode)
    
    results["tests"].append({
        "name": "Toggle to Streaming",
        "previous_mode": prev_mode,
        "new_mode": new_mode,
        "passed": new_mode == "streaming"
    })
    
    # Test 3: Start streaming
    start_error = None
    start_success = False
    try:
        start_success = start_streaming()
    except Exception as e:
        start_error = str(e)
    
    # Log the attempt
    stream_debugger.log_process_start_attempt(
        success=start_success, 
        error_msg=start_error if start_error else None
    )
    
    # Check streaming status
    time.sleep(2)  # Give time for processes to start
    status_after_start = get_streaming_status()
    
    results["tests"].append({
        "name": "Start Streaming Process",
        "success": start_success,
        "error": start_error,
        "status": status_after_start,
        "passed": start_success and status_after_start["is_streaming"]
    })
    
    # Test 4: Toggle back to batch mode
    prev_mode = st.session_state["data_mode"]
    st.session_state["data_mode"] = "batch"
    new_mode = st.session_state["data_mode"]
    
    # Log the change
    stream_debugger.log_session_state_change(prev_mode, new_mode)
    
    results["tests"].append({
        "name": "Toggle to Batch",
        "previous_mode": prev_mode,
        "new_mode": new_mode,
        "passed": new_mode == "batch"
    })
    
    # Test 5: Stop streaming
    stop_error = None
    stop_success = False
    try:
        stop_success = stop_streaming()
    except Exception as e:
        stop_error = str(e)
    
    # Check final status
    time.sleep(2)  # Give time for processes to stop
    final_status = get_streaming_status()
    
    results["tests"].append({
        "name": "Stop Streaming Process",
        "success": stop_success,
        "error": stop_error,
        "status": final_status,
        "passed": stop_success and not final_status["is_streaming"]
    })
    
    # Calculate overall success
    results["all_tests_passed"] = all(test["passed"] for test in results["tests"])
    
    return results


def render_debug_ui():
    """Render a debug UI in Streamlit"""
    st.subheader("Streaming Debug Information")
    
    # Check current session state
    current_mode = st.session_state.get("data_mode", "Not set")
    
    st.write(f"Current data mode: **{current_mode}**")
    
    # Check streaming status
    status = get_streaming_status()
    
    st.write("Streaming Status:")
    st.json(status)
    
    # Create tabs for different debugging tasks
    debug_tab1, debug_tab2, debug_tab3 = st.tabs(["Process Tests", "Toggle Tests", "Debug Logs"])
    
    with debug_tab1:
        # Show detailed process check
        if st.button("Check Processes", key="check_processes_btn"):
            process_check = stream_debugger.check_processes()
            st.write("Process Check Results:")
            st.json(process_check)
        
        # Add kafka process test button
        if st.button("Run Kafka Process Launch Test", key="process_launch_btn"):
            with st.spinner("Testing Kafka process launch..."):
                test_results = test_kafka_process_launch()
                st.write("Test Results:")
                if test_results["all_tests_passed"]:
                    st.success("All tests passed! Kafka processes are working correctly.")
                else:
                    st.error("Some tests failed. See details below.")
                st.json(test_results)
    
    with debug_tab2:
        # Add streaming toggle test button
        if st.button("Test Streaming Toggle", key="toggle_test_btn"):
            with st.spinner("Testing mode toggle and streaming processes..."):
                toggle_results = test_streaming_toggle()
                st.write("Test Results:")
                if toggle_results["all_tests_passed"]:
                    st.success("All toggle tests passed! Mode switching is working correctly.")
                else:
                    st.error("Some toggle tests failed. See details below.")
                st.json(toggle_results)
    
    with debug_tab3:
        # Display logs
        if st.checkbox("Show Session State Logs", key="session_logs_chk"):
            logs_df = stream_debugger.get_session_state_logs()
            if not logs_df.empty:
                st.dataframe(logs_df)
            else:
                st.info("No session state logs recorded yet")
        
        if st.checkbox("Show Process Logs", key="process_logs_chk"):
            logs_df = stream_debugger.get_process_logs()
            if not logs_df.empty:
                st.dataframe(logs_df)
            else:
                st.info("No process logs recorded yet")


def print_debug_summary():
    """Print a debug summary to the console"""
    print("=== STREAM DEBUGGING SUMMARY ===")
    
    # Check session state
    current_mode = st.session_state.get("data_mode", "Not set")
    print(f"Current data mode: {current_mode}")
    
    # Check streaming status
    status = get_streaming_status()
    print(f"Is streaming: {status['is_streaming']}")
    print(f"Producer status: {status['producer']}")
    print(f"Consumer status: {status['consumer']}")
    
    # If in mock mode, note that
    if status.get("mock_mode", False):
        print(f"Mock mode: {status['mock_mode']}")
        print("Process checking skipped in mock mode")
    else:
        # Only try to check processes if not in mock mode
        try:
            # Use our check_processes method which handles errors better
            process_check = stream_debugger.check_processes()
            if "python_processes" in process_check:
                print(f"Python processes found: {process_check['python_processes']}")
        except Exception as e:
            print(f"Process checking skipped: {str(e)}")
    
    print("===============================") 