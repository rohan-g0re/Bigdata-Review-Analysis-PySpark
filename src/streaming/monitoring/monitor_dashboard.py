"""
System monitoring dashboard for the streaming pipeline

This module implements a Streamlit dashboard for monitoring the
real-time performance and health of the streaming system.
"""
import os
import json
import time
import pandas as pd
import streamlit as st
import altair as alt
from datetime import datetime, timedelta
import threading
import subprocess
import psutil
import logging
from typing import Dict, Any, List, Optional, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("monitor_dashboard")

class SystemMonitor:
    """
    Monitors system metrics and component status
    """
    def __init__(self, metrics_dir: str = "data/metrics"):
        self.metrics_dir = metrics_dir
        self.metrics_history = {
            "timestamp": [],
            "cpu_percent": [],
            "memory_percent": [],
            "producer_throughput": [],
            "consumer_throughput": [],
            "latency": [],
            "queue_size": [],
            "error_count": []
        }
        self.component_status = {
            "producer": "unknown",
            "consumer": "unknown",
            "kafka": "unknown"
        }
        self.running = False
        self.monitor_thread = None
        
        # Ensure metrics directory exists
        os.makedirs(metrics_dir, exist_ok=True)
    
    def start_monitoring(self):
        """Start the background monitoring thread"""
        self.running = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
        logger.info("Monitoring thread started")
    
    def stop_monitoring(self):
        """Stop the background monitoring thread"""
        self.running = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=2)
        logger.info("Monitoring thread stopped")
    
    def _monitor_loop(self):
        """
        Background loop that collects system metrics
        and component status at regular intervals
        """
        while self.running:
            try:
                # Collect system metrics
                self._collect_system_metrics()
                
                # Check component status
                self._check_component_status()
                
                # Save metrics to disk
                self._save_metrics()
                
                # Sleep for a while
                time.sleep(5)
            except Exception as e:
                logger.error(f"Error in monitoring loop: {str(e)}")
                time.sleep(10)  # Wait longer on error
    
    def _collect_system_metrics(self):
        """Collect system metrics such as CPU and memory usage"""
        now = datetime.now()
        
        # System metrics
        cpu_percent = psutil.cpu_percent()
        memory_percent = psutil.virtual_memory().percent
        
        # Application metrics (simulate for now, would be collected from the real system)
        producer_throughput = self._get_producer_throughput()
        consumer_throughput = self._get_consumer_throughput()
        latency = self._get_latency()
        queue_size = self._get_queue_size()
        error_count = self._get_error_count()
        
        # Add to history
        self.metrics_history["timestamp"].append(now)
        self.metrics_history["cpu_percent"].append(cpu_percent)
        self.metrics_history["memory_percent"].append(memory_percent)
        self.metrics_history["producer_throughput"].append(producer_throughput)
        self.metrics_history["consumer_throughput"].append(consumer_throughput)
        self.metrics_history["latency"].append(latency)
        self.metrics_history["queue_size"].append(queue_size)
        self.metrics_history["error_count"].append(error_count)
        
        # Keep only the latest 1000 points
        max_history = 1000
        if len(self.metrics_history["timestamp"]) > max_history:
            for key in self.metrics_history:
                self.metrics_history[key] = self.metrics_history[key][-max_history:]
    
    def _check_component_status(self):
        """Check the status of system components"""
        # Producer status (check if process is running)
        self.component_status["producer"] = self._check_process_status("python", "kafka_producer.py")
        
        # Consumer status
        self.component_status["consumer"] = self._check_process_status("python", "kafka_consumer.py")
        
        # Kafka status
        self.component_status["kafka"] = self._check_kafka_status()
    
    def _check_process_status(self, process_name: str, search_str: str) -> str:
        """Check if a process is running by name and search string"""
        try:
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                if proc.info['name'] and process_name in proc.info['name'].lower():
                    if proc.info['cmdline'] and any(search_str in cmd.lower() for cmd in proc.info['cmdline']):
                        return "running"
            return "stopped"
        except:
            return "unknown"
    
    def _check_kafka_status(self) -> str:
        """Check if Kafka is running"""
        try:
            # Look for Kafka process
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                if proc.info['name'] and "java" in proc.info['name'].lower():
                    if proc.info['cmdline'] and any("kafka" in cmd.lower() for cmd in proc.info['cmdline']):
                        return "running"
            
            # Check if port 9092 is in use
            for conn in psutil.net_connections():
                if conn.laddr.port == 9092:
                    return "running"
                    
            return "stopped"
        except:
            return "unknown"
    
    def _get_producer_throughput(self) -> float:
        """Get producer throughput (simulated or from metrics file)"""
        # In a real system, this would read from a metrics file or API
        # For now, simulate some realistic values
        try:
            # Check if metrics file exists
            producer_metrics_file = os.path.join(self.metrics_dir, "producer_metrics.json")
            if os.path.exists(producer_metrics_file):
                with open(producer_metrics_file, 'r') as f:
                    metrics = json.load(f)
                    return metrics.get("throughput", 0)
            
            # Otherwise return a simulated value
            if self.metrics_history["producer_throughput"]:
                last_val = self.metrics_history["producer_throughput"][-1]
                # Simulate some variance around the last value
                return max(0, last_val + (last_val * 0.1 * (hash(datetime.now()) % 10 - 5) / 5))
            else:
                return 100  # Default starting value
        except:
            return 0
    
    def _get_consumer_throughput(self) -> float:
        """Get consumer throughput (simulated or from metrics file)"""
        try:
            # Check if metrics file exists
            consumer_metrics_file = os.path.join(self.metrics_dir, "consumer_metrics.json")
            if os.path.exists(consumer_metrics_file):
                with open(consumer_metrics_file, 'r') as f:
                    metrics = json.load(f)
                    return metrics.get("throughput", 0)
            
            # Otherwise return a simulated value that's slightly less than producer throughput
            if self.metrics_history["producer_throughput"]:
                producer_val = self.metrics_history["producer_throughput"][-1]
                # Consumer is usually slightly behind producer
                return max(0, producer_val * 0.95 * (1 + (hash(datetime.now()) % 10 - 5) / 50))
            else:
                return 95  # Default starting value
        except:
            return 0
    
    def _get_latency(self) -> float:
        """Get end-to-end latency in seconds (simulated or from metrics file)"""
        try:
            # Check if metrics file exists
            latency_metrics_file = os.path.join(self.metrics_dir, "latency_metrics.json")
            if os.path.exists(latency_metrics_file):
                with open(latency_metrics_file, 'r') as f:
                    metrics = json.load(f)
                    return metrics.get("latency", 0)
            
            # Otherwise return a simulated value
            if self.metrics_history["latency"]:
                last_val = self.metrics_history["latency"][-1]
                # Simulate some variance around the last value
                return max(0.1, min(30, last_val + (last_val * 0.1 * (hash(datetime.now()) % 10 - 5) / 5)))
            else:
                return 5  # Default starting value
        except:
            return 0
    
    def _get_queue_size(self) -> int:
        """Get Kafka queue size (simulated or from metrics file)"""
        try:
            # Check if metrics file exists
            queue_metrics_file = os.path.join(self.metrics_dir, "queue_metrics.json")
            if os.path.exists(queue_metrics_file):
                with open(queue_metrics_file, 'r') as f:
                    metrics = json.load(f)
                    return metrics.get("queue_size", 0)
            
            # Otherwise return a simulated value
            if self.metrics_history["queue_size"]:
                last_val = self.metrics_history["queue_size"][-1]
                # Simulate some variance based on throughput difference
                if len(self.metrics_history["producer_throughput"]) > 0 and len(self.metrics_history["consumer_throughput"]) > 0:
                    prod = self.metrics_history["producer_throughput"][-1]
                    cons = self.metrics_history["consumer_throughput"][-1]
                    diff = prod - cons
                    return max(0, int(last_val + diff * 0.1))
                else:
                    return last_val
            else:
                return 100  # Default starting value
        except:
            return 0
    
    def _get_error_count(self) -> int:
        """Get error count (simulated or from metrics file)"""
        try:
            # Check if metrics file exists
            error_metrics_file = os.path.join(self.metrics_dir, "error_metrics.json")
            if os.path.exists(error_metrics_file):
                with open(error_metrics_file, 'r') as f:
                    metrics = json.load(f)
                    return metrics.get("error_count", 0)
            
            # Otherwise return a simulated value
            if self.metrics_history["error_count"]:
                # Occasionally increment error count
                if hash(datetime.now()) % 100 == 0:
                    return self.metrics_history["error_count"][-1] + 1
                else:
                    return self.metrics_history["error_count"][-1]
            else:
                return 0  # Default starting value
        except:
            return 0
    
    def _save_metrics(self):
        """Save metrics to disk for persistence"""
        try:
            # Create a snapshot of current metrics
            now = datetime.now()
            if not self.metrics_history["timestamp"]:
                return
                
            latest_idx = -1
            snapshot = {
                "timestamp": now.isoformat(),
                "cpu_percent": self.metrics_history["cpu_percent"][latest_idx],
                "memory_percent": self.metrics_history["memory_percent"][latest_idx],
                "producer_throughput": self.metrics_history["producer_throughput"][latest_idx],
                "consumer_throughput": self.metrics_history["consumer_throughput"][latest_idx],
                "latency": self.metrics_history["latency"][latest_idx],
                "queue_size": self.metrics_history["queue_size"][latest_idx],
                "error_count": self.metrics_history["error_count"][latest_idx],
                "component_status": self.component_status
            }
            
            # Save to a file
            snapshot_file = os.path.join(self.metrics_dir, "metrics_snapshot.json")
            with open(snapshot_file, 'w') as f:
                json.dump(snapshot, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving metrics: {str(e)}")
    
    def get_metrics_dataframe(self, window_minutes: int = 30) -> pd.DataFrame:
        """
        Get metrics history as a DataFrame
        
        Args:
            window_minutes: Time window in minutes to include
            
        Returns:
            DataFrame with metrics history
        """
        if not self.metrics_history["timestamp"]:
            return pd.DataFrame()
            
        cutoff_time = datetime.now() - timedelta(minutes=window_minutes)
        
        # Filter metrics by time window
        filtered_data = {key: [] for key in self.metrics_history.keys()}
        
        for i, ts in enumerate(self.metrics_history["timestamp"]):
            if ts >= cutoff_time:
                for key in self.metrics_history:
                    if i < len(self.metrics_history[key]):
                        filtered_data[key].append(self.metrics_history[key][i])
        
        return pd.DataFrame(filtered_data)
    
    def get_component_status(self) -> Dict[str, str]:
        """Get the current status of all components"""
        return self.component_status.copy()
    
    def get_system_health(self) -> Dict[str, Any]:
        """
        Get overall system health assessment
        
        Returns:
            Dict with health status and issues
        """
        if not self.metrics_history["timestamp"]:
            return {
                "status": "unknown",
                "issues": ["No metrics data available"]
            }
            
        issues = []
        
        # Check component status
        for component, status in self.component_status.items():
            if status != "running":
                issues.append(f"{component} is not running (status: {status})")
        
        # Check metrics
        if self.metrics_history["cpu_percent"] and self.metrics_history["cpu_percent"][-1] > 80:
            issues.append(f"High CPU usage: {self.metrics_history['cpu_percent'][-1]:.1f}%")
        
        if self.metrics_history["memory_percent"] and self.metrics_history["memory_percent"][-1] > 80:
            issues.append(f"High memory usage: {self.metrics_history['memory_percent'][-1]:.1f}%")
        
        if self.metrics_history["latency"] and self.metrics_history["latency"][-1] > 10:
            issues.append(f"High latency: {self.metrics_history['latency'][-1]:.1f} seconds")
        
        if self.metrics_history["queue_size"] and self.metrics_history["queue_size"][-1] > 1000:
            issues.append(f"Large queue size: {self.metrics_history['queue_size'][-1]} messages")
        
        # Determine overall status
        if len(issues) == 0:
            status = "healthy"
        elif len(issues) <= 2:
            status = "warning"
        else:
            status = "critical"
        
        return {
            "status": status,
            "issues": issues
        }

def render_dashboard():
    """
    Render the Streamlit monitoring dashboard
    """
    st.set_page_config(
        page_title="Steam Reviews Streaming Monitor",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    st.title("Steam Reviews Streaming System Monitor")
    
    # Initialize or get the monitor from session state
    if "monitor" not in st.session_state:
        st.session_state.monitor = SystemMonitor()
        st.session_state.monitor.start_monitoring()
    
    monitor = st.session_state.monitor
    
    # Time window selector
    st.sidebar.title("Dashboard Settings")
    time_window = st.sidebar.slider(
        "Time Window (minutes)",
        min_value=5,
        max_value=120,
        value=30,
        step=5
    )
    
    # Auto-refresh checkbox
    auto_refresh = st.sidebar.checkbox("Auto-refresh", value=True)
    
    # Manual refresh button
    if st.sidebar.button("Refresh Now"):
        st.experimental_rerun()
    
    # Get latest metrics
    metrics_df = monitor.get_metrics_dataframe(window_minutes=time_window)
    component_status = monitor.get_component_status()
    system_health = monitor.get_system_health()
    
    # System Health Indicators
    st.subheader("System Health")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        health_color = {
            "healthy": "green",
            "warning": "orange",
            "critical": "red",
            "unknown": "gray"
        }.get(system_health["status"], "gray")
        
        st.markdown(
            f"<h1 style='text-align: center; color: {health_color};'>"
            f"Status: {system_health['status'].upper()}</h1>",
            unsafe_allow_html=True
        )
    
    with col2:
        st.subheader("Component Status")
        status_df = pd.DataFrame({
            "Component": list(component_status.keys()),
            "Status": list(component_status.values())
        })
        st.dataframe(status_df)
    
    with col3:
        st.subheader("Issues")
        if system_health["issues"]:
            for issue in system_health["issues"]:
                st.warning(issue)
        else:
            st.success("No issues detected")
    
    # Metrics Charts
    st.subheader("System Metrics")
    
    if not metrics_df.empty:
        # Convert timestamp column to datetime if it's not already
        if not pd.api.types.is_datetime64_any_dtype(metrics_df["timestamp"]):
            metrics_df["timestamp"] = pd.to_datetime(metrics_df["timestamp"])
        
        # Create two rows of charts
        row1_col1, row1_col2 = st.columns(2)
        row2_col1, row2_col2 = st.columns(2)
        
        with row1_col1:
            st.subheader("Throughput")
            # Create a melted DataFrame for throughput chart
            throughput_data = metrics_df[["timestamp", "producer_throughput", "consumer_throughput"]].melt(
                id_vars=["timestamp"],
                var_name="Component",
                value_name="Throughput (msgs/sec)"
            )
            
            # Create throughput chart
            throughput_chart = alt.Chart(throughput_data).mark_line().encode(
                x=alt.X("timestamp:T", title="Time"),
                y=alt.Y("Throughput (msgs/sec):Q"),
                color=alt.Color("Component:N", legend=alt.Legend(title="Component"))
            ).properties(height=300)
            
            st.altair_chart(throughput_chart, use_container_width=True)
        
        with row1_col2:
            st.subheader("Latency")
            # Create latency chart
            latency_chart = alt.Chart(metrics_df).mark_line(color="red").encode(
                x=alt.X("timestamp:T", title="Time"),
                y=alt.Y("latency:Q", title="Latency (seconds)")
            ).properties(height=300)
            
            # Add a threshold line for target latency (30 seconds)
            threshold = alt.Chart(pd.DataFrame({"threshold": [30]})).mark_rule(color="orange", strokeDash=[3, 3]).encode(
                y="threshold:Q"
            )
            
            st.altair_chart(latency_chart + threshold, use_container_width=True)
        
        with row2_col1:
            st.subheader("System Resources")
            # Create a melted DataFrame for system resources
            resources_data = metrics_df[["timestamp", "cpu_percent", "memory_percent"]].melt(
                id_vars=["timestamp"],
                var_name="Resource",
                value_name="Usage (%)"
            )
            
            # Create system resources chart
            resources_chart = alt.Chart(resources_data).mark_line().encode(
                x=alt.X("timestamp:T", title="Time"),
                y=alt.Y("Usage (%):Q", scale=alt.Scale(domain=[0, 100])),
                color=alt.Color("Resource:N", legend=alt.Legend(title="Resource"))
            ).properties(height=300)
            
            st.altair_chart(resources_chart, use_container_width=True)
        
        with row2_col2:
            st.subheader("Queue Size")
            # Create queue size chart
            queue_chart = alt.Chart(metrics_df).mark_area(color="blue", opacity=0.5).encode(
                x=alt.X("timestamp:T", title="Time"),
                y=alt.Y("queue_size:Q", title="Queue Size (messages)")
            ).properties(height=300)
            
            st.altair_chart(queue_chart, use_container_width=True)
    else:
        st.warning("No metrics data available. Waiting for data...")
    
    # Auto-refresh the page if enabled
    if auto_refresh:
        # Refresh every 5 seconds
        time.sleep(1)
        st.experimental_rerun()

if __name__ == "__main__":
    render_dashboard() 