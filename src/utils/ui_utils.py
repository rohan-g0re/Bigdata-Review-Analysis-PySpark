"""
UI utilities for the Streamlit dashboard
"""
import streamlit as st
import time
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Callable
import logging

from src.utils.stream_control import start_streaming, stop_streaming, is_streaming, get_streaming_status
from src.streaming.config.settings import DASHBOARD_REFRESH_INTERVAL
from src.utils.debug_utils import stream_debugger

logger = logging.getLogger(__name__)

def create_refresh_button(key: str = "refresh_button", text: str = "Refresh Data"):
    """Create a standardized refresh button"""
    return st.button(text, key=key)

def create_data_mode_toggle(session_key: str = "data_mode"):
    """
    Create a toggle between batch and streaming modes
    
    Args:
        session_key: Session state key to store the mode
        
    Returns:
        str: Current mode ('batch' or 'streaming')
    """
    # Initialize session state if needed
    if session_key not in st.session_state:
        st.session_state[session_key] = "batch"
    
    # Keep track of previous mode to detect changes
    prev_mode = st.session_state[session_key]
    
    # Create the toggle
    col1, col2 = st.columns([3, 1])
    
    with col1:
        mode = st.radio(
            "Data Mode:",
            options=["Batch Analysis", "Real-time Streaming"],
            index=0 if st.session_state[session_key] == "batch" else 1,
            horizontal=True,
            key=f"{session_key}_radio"
        )
    
    # Update session state
    new_mode = "batch" if mode == "Batch Analysis" else "streaming"
    st.session_state[session_key] = new_mode
    
    # Check if mode changed
    if prev_mode != new_mode:
        # Log session state change for debugging
        stream_debugger.log_session_state_change(prev_mode, new_mode)
        
        if new_mode == "streaming":
            # Start streaming
            with st.spinner("Starting Kafka streaming..."):
                try:
                    success = start_streaming()
                    if success:
                        st.success("Streaming started successfully!")
                        # Log successful process start
                        stream_debugger.log_process_start_attempt(success=True)
                    else:
                        st.error("Failed to start streaming. See logs for details.")
                        # Log failed process start
                        stream_debugger.log_process_start_attempt(success=False, error_msg="start_streaming() returned False")
                except Exception as e:
                    st.error(f"Exception during streaming start: {str(e)}")
                    # Log exception during process start
                    stream_debugger.log_process_start_attempt(success=False, error_msg=str(e))
        else:
            # Stop streaming
            with st.spinner("Stopping Kafka streaming..."):
                success = stop_streaming()
                if success:
                    st.info("Streaming stopped successfully.")
                else:
                    st.error("Failed to stop streaming. See logs for details.")
    
    return st.session_state[session_key]

def create_streaming_status_indicators():
    """
    Display streaming system status indicators
    
    Returns:
        Dict: Status information
    """
    status = get_streaming_status()
    
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Producer**")
        st.markdown("**Consumer**")
    with col2:
        # Show green circle for running, red circle for stopped, gray for unknown
        producer_icon = "🟢" if status["producer"] == "running" else "🔴"
        consumer_icon = "🟢" if status["consumer"] == "running" else "🔴"
        
        st.markdown(f"{producer_icon} {status['producer'].capitalize()}")
        st.markdown(f"{consumer_icon} {status['consumer'].capitalize()}")
    
    return status

def display_data_freshness(freshness_info: Dict[str, Any], container = None):
    """
    Display data freshness indicator
    
    Args:
        freshness_info: Dictionary with freshness information
        container: Streamlit container to use (optional)
    """
    target = container if container else st
    
    if not freshness_info["has_data"]:
        target.warning("No streaming data available. Start the streaming system to see real-time data.")
        return
    
    status = freshness_info["status"]
    last_refresh = freshness_info.get("last_refresh")
    
    if status == "not_refreshed":
        target.info("Data not refreshed yet")
    elif status == "fresh":
        target.success("Data is fresh (less than 30 seconds old)")
    elif status == "recent":
        target.info("Data was updated recently (less than 2 minutes ago)")
    elif status == "stale":
        target.warning("Data may be stale (last updated more than 2 minutes ago)")
    
    if last_refresh:
        target.caption(f"Last updated: {last_refresh.strftime('%Y-%m-%d %H:%M:%S')}")

def create_auto_refresh_toggle(
    session_key: str = "auto_refresh", 
    default: bool = True
) -> bool:
    """
    Create a toggle for auto-refresh functionality
    
    Args:
        session_key: Session state key for the toggle
        default: Default toggle state
        
    Returns:
        bool: Current auto-refresh state
    """
    # Initialize session state if needed
    if session_key not in st.session_state:
        st.session_state[session_key] = default
    
    auto_refresh = st.checkbox(
        "Auto-refresh data",
        value=st.session_state[session_key],
        key=f"{session_key}_checkbox",
        help=f"Automatically refresh data every {DASHBOARD_REFRESH_INTERVAL} seconds"
    )
    
    # Update session state
    st.session_state[session_key] = auto_refresh
    
    return auto_refresh

def run_with_spinner(
    fn: Callable, 
    text: str = "Loading data...", 
    args: tuple = (), 
    kwargs: Dict[str, Any] = {}
):
    """
    Run a function with a spinner
    
    Args:
        fn: Function to run
        text: Spinner text
        args: Function arguments
        kwargs: Function keyword arguments
        
    Returns:
        The result of the function
    """
    with st.spinner(text):
        return fn(*args, **kwargs)

def auto_refresh_component(
    refresh_interval: int = DASHBOARD_REFRESH_INTERVAL,
    session_key: str = "last_auto_refresh"
):
    """
    Create an auto-refresh component
    
    Args:
        refresh_interval: Refresh interval in seconds
        session_key: Session state key for tracking
        
    Returns:
        bool: True if a refresh should occur
    """
    # Initialize session state if needed
    if session_key not in st.session_state:
        st.session_state[session_key] = datetime.now()
        return True
    
    # Check if it's time to refresh
    now = datetime.now()
    time_elapsed = (now - st.session_state[session_key]).total_seconds()
    
    if time_elapsed >= refresh_interval:
        # Update timestamp and return True to indicate refresh needed
        st.session_state[session_key] = now
        
        # Show a subtle indicator that refresh is happening
        refresh_indicator = st.empty()
        with refresh_indicator.container():
            # Display a progress bar for a brief moment
            progress = st.progress(0)
            for i in range(10):
                progress.progress((i + 1) * 10)
                time.sleep(0.01)  # Very brief delay
            
            # Clear the progress bar
            progress.empty()
        
        return True
    
    # Add a countdown indicator
    time_to_next_refresh = refresh_interval - time_elapsed
    st.caption(f"Next refresh in {int(time_to_next_refresh)} seconds")
    
    return False 