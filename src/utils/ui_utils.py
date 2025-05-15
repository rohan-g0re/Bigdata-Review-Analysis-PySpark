"""
UI utilities for the Streamlit dashboard
"""
import streamlit as st
import time
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Callable

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
    st.session_state[session_key] = "batch" if mode == "Batch Analysis" else "streaming"
    
    return st.session_state[session_key]

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
        help="Automatically refresh data at regular intervals"
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
    refresh_interval: int = 10,
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
    
    # Always update the session state to force refresh
    st.session_state[session_key] = datetime.now()
    
    # Create a hidden progress bar for triggering refresh
    progress_bar = st.empty()
    progress_bar.progress(0, "Refreshing...")
    
    # Simulate progress
    for i in range(10):
        time.sleep(0.02)  # Short delay for visual feedback
        progress_bar.progress((i + 1) * 10, "Refreshing...")
    
    # Clear the progress bar
    progress_bar.empty()
    
    # Always return True to force refresh
    return True 