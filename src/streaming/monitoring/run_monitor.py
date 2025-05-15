"""
Run script for the system monitoring dashboard

This script starts the system monitoring dashboard.
"""
import os
import sys
import argparse
import subprocess
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("monitor_runner")

def run_monitor_dashboard(port: int = 8501):
    """
    Run the system monitoring dashboard
    
    Args:
        port: Port to run the dashboard on
    """
    try:
        # Get the path to the dashboard script
        dashboard_script_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "monitor_dashboard.py"
        )
        
        logger.info(f"Starting monitoring dashboard on port {port}")
        
        # Build the command to run the dashboard
        command = [
            "streamlit", "run", dashboard_script_path,
            "--server.port", str(port),
            "--server.address", "0.0.0.0"
        ]
        
        # Run the command
        process = subprocess.Popen(command)
        
        # Return the process object so it can be terminated later if needed
        return process
        
    except Exception as e:
        logger.error(f"Error starting monitoring dashboard: {str(e)}")
        return None

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Run the system monitoring dashboard")
    parser.add_argument(
        "--port",
        type=int,
        default=8502,
        help="Port to run the dashboard on (default: 8502)"
    )
    
    args = parser.parse_args()
    
    # Run the dashboard
    process = run_monitor_dashboard(port=args.port)
    
    if process:
        try:
            # Keep running until interrupted
            process.wait()
        except KeyboardInterrupt:
            logger.info("Stopping monitoring dashboard")
            process.terminate()
            process.wait()
    else:
        sys.exit(1)

if __name__ == "__main__":
    main() 