"""
Final testing script for the complete streaming system

This script runs a series of tests with different data volumes and rates
to verify system performance and reliability.

Usage:
    python -m src.streaming.final_test
"""
import os
import sys
import time
import json
import argparse
import logging
import subprocess
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional

from src.streaming.utils.logging_config import setup_logger
from src.streaming.test_integration import run_integration_test
from src.streaming.optimization.performance_optimizer import PerformanceAnalyzer, apply_optimized_configuration

# Set up logging
logger = setup_logger("final_test", "final_test.log")

# Test configurations
TEST_CONFIGURATIONS = [
    {
        "name": "small_slow",
        "data_volume": "small",
        "rate": "slow",
        "duration": 60,
        "description": "Small data volume with slow rate"
    },
    {
        "name": "small_fast",
        "data_volume": "small",
        "rate": "fast",
        "duration": 60,
        "description": "Small data volume with fast rate"
    },
    {
        "name": "medium_normal",
        "data_volume": "medium",
        "rate": "normal",
        "duration": 120,
        "description": "Medium data volume with normal rate"
    },
    {
        "name": "large_normal",
        "data_volume": "large",
        "rate": "normal",
        "duration": 180,
        "description": "Large data volume with normal rate"
    },
    {
        "name": "recovery_test",
        "data_volume": "medium",
        "rate": "normal",
        "duration": 120,
        "with_interruption": True,
        "interruption_time": 30,  # seconds into test
        "interruption_component": "consumer",
        "description": "Tests system recovery after component failure"
    }
]

def run_test(
    test_config: Dict[str, Any],
    output_dir: str = "test_results"
) -> Dict[str, Any]:
    """
    Run a test with the given configuration
    
    Args:
        test_config: Test configuration
        output_dir: Directory to save test results
        
    Returns:
        Dict with test results
    """
    test_name = test_config["name"]
    logger.info(f"Starting test: {test_name} - {test_config['description']}")
    
    # Create output directory for this test
    test_output_dir = os.path.join(output_dir, test_name)
    os.makedirs(test_output_dir, exist_ok=True)
    
    # Record test start time
    start_time = datetime.now()
    
    # Configure interruption if needed
    if test_config.get("with_interruption", False):
        interrupt_component = test_config.get("interruption_component", "consumer")
        interrupt_time = test_config.get("interruption_time", 30)
        
        # Set up interruption in a separate thread
        import threading
        def interrupt_component():
            logger.info(f"Sleeping for {interrupt_time} seconds before interrupting {interrupt_component}")
            time.sleep(interrupt_time)
            
            logger.info(f"Interrupting {interrupt_component}")
            # Find and kill the component process
            # This is simplified - in a real system we would have a more robust way to do this
            if interrupt_component == "consumer":
                os.system("pkill -f 'python.*kafka_consumer'")
            elif interrupt_component == "producer":
                os.system("pkill -f 'python.*kafka_producer'")
                
            logger.info(f"Interrupted {interrupt_component}, waiting 10 seconds")
            time.sleep(10)
            
            # Restart the component
            logger.info(f"Restarting {interrupt_component}")
            if interrupt_component == "consumer":
                subprocess.Popen(["python", "-m", "src.streaming.run_system", "--component", "consumer"])
            elif interrupt_component == "producer":
                subprocess.Popen(["python", "-m", "src.streaming.run_system", "--component", "producer"])
            
            logger.info(f"Restarted {interrupt_component}")
            
        # Start interruption thread
        interrupt_thread = threading.Thread(target=interrupt_component)
        interrupt_thread.daemon = True
        interrupt_thread.start()
    
    # Run the integration test
    report, passed = run_integration_test(
        data_volume=test_config["data_volume"],
        rate=test_config["rate"],
        duration=test_config["duration"]
    )
    
    # Record test end time
    end_time = datetime.now()
    test_duration = (end_time - start_time).total_seconds()
    
    # Prepare test results
    test_results = {
        "test_name": test_name,
        "description": test_config["description"],
        "configuration": test_config,
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "test_duration_seconds": test_duration,
        "passed": passed,
        "metrics": report if report else {}
    }
    
    # Save test results
    results_file = os.path.join(test_output_dir, "test_results.json")
    with open(results_file, 'w') as f:
        json.dump(test_results, f, indent=2)
    
    logger.info(f"Test {test_name} completed in {test_duration:.2f} seconds, passed: {passed}")
    
    return test_results

def run_all_tests(output_dir: str = "test_results") -> List[Dict[str, Any]]:
    """
    Run all defined tests
    
    Args:
        output_dir: Directory to save test results
        
    Returns:
        List of test result dictionaries
    """
    os.makedirs(output_dir, exist_ok=True)
    
    results = []
    
    for config in TEST_CONFIGURATIONS:
        try:
            test_result = run_test(config, output_dir)
            results.append(test_result)
            
            # Wait between tests
            time.sleep(10)
            
        except Exception as e:
            logger.error(f"Error running test {config['name']}: {str(e)}")
            results.append({
                "test_name": config["name"],
                "error": str(e),
                "passed": False
            })
    
    return results

def generate_final_report(test_results: List[Dict[str, Any]], output_dir: str = "test_results") -> Dict[str, Any]:
    """
    Generate a final report of all test results
    
    Args:
        test_results: List of test results
        output_dir: Directory to save the report
        
    Returns:
        Dict with final report
    """
    passed_tests = sum(1 for result in test_results if result.get("passed", False))
    total_tests = len(test_results)
    
    # Calculate average metrics across all successful tests
    avg_metrics = {
        "avg_producer_throughput": 0,
        "avg_consumer_throughput": 0,
        "avg_latency": 0,
        "max_latency": 0
    }
    
    successful_tests = [result for result in test_results if result.get("passed", False)]
    
    for result in successful_tests:
        metrics = result.get("metrics", {})
        if "producer" in metrics and "throughput_records_per_second" in metrics["producer"]:
            avg_metrics["avg_producer_throughput"] += metrics["producer"]["throughput_records_per_second"]
        
        if "consumer" in metrics and "throughput_records_per_second" in metrics["consumer"]:
            avg_metrics["avg_consumer_throughput"] += metrics["consumer"]["throughput_records_per_second"]
        
        if "latency" in metrics:
            if "average_seconds" in metrics["latency"]:
                avg_metrics["avg_latency"] += metrics["latency"]["average_seconds"]
            if "max_seconds" in metrics["latency"]:
                avg_metrics["max_latency"] = max(avg_metrics["max_latency"], metrics["latency"]["max_seconds"])
    
    # Calculate averages
    if successful_tests:
        avg_metrics["avg_producer_throughput"] /= len(successful_tests)
        avg_metrics["avg_consumer_throughput"] /= len(successful_tests)
        avg_metrics["avg_latency"] /= len(successful_tests)
    
    # Generate report
    report = {
        "timestamp": datetime.now().isoformat(),
        "total_tests": total_tests,
        "passed_tests": passed_tests,
        "pass_rate": passed_tests / total_tests if total_tests > 0 else 0,
        "average_metrics": avg_metrics,
        "test_results": test_results
    }
    
    # Save report
    report_file = os.path.join(output_dir, "final_report.json")
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)
    
    # Generate optimization recommendations if we have test data
    if successful_tests:
        analyzer = PerformanceAnalyzer()
        
        # Load each test result into the analyzer
        for result in successful_tests:
            test_result_file = os.path.join(output_dir, result["test_name"], "test_results.json")
            if os.path.exists(test_result_file):
                analyzer.load_test_results(test_result_file)
        
        # Generate optimization report
        optimization_report = analyzer.generate_optimization_report()
        
        # Save optimization report
        optimization_file = os.path.join(output_dir, "optimization_report.json")
        with open(optimization_file, 'w') as f:
            json.dump(optimization_report, f, indent=2)
        
        # Add optimization recommendations to final report
        report["optimization_recommendations"] = optimization_report.get("summary", "")
    
    return report

def log_final_report(report: Dict[str, Any]):
    """
    Log the final report summary
    
    Args:
        report: Final report dictionary
    """
    logger.info("FINAL TEST REPORT")
    logger.info("=================")
    logger.info(f"Total tests: {report['total_tests']}")
    logger.info(f"Passed tests: {report['passed_tests']}")
    logger.info(f"Pass rate: {report['pass_rate'] * 100:.1f}%")
    logger.info("")
    logger.info("Average Metrics:")
    logger.info(f"  Producer throughput: {report['average_metrics']['avg_producer_throughput']:.2f} records/sec")
    logger.info(f"  Consumer throughput: {report['average_metrics']['avg_consumer_throughput']:.2f} records/sec")
    logger.info(f"  Average latency: {report['average_metrics']['avg_latency']:.2f} seconds")
    logger.info(f"  Maximum latency: {report['average_metrics']['max_latency']:.2f} seconds")
    
    if "optimization_recommendations" in report:
        logger.info("")
        logger.info("Optimization Recommendations:")
        logger.info(f"  {report['optimization_recommendations']}")
    
    logger.info("")
    logger.info("Test Results:")
    for result in report["test_results"]:
        status = "PASSED" if result.get("passed", False) else "FAILED"
        logger.info(f"  {result['test_name']}: {status}")

def main():
    """Main function"""
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Run final tests for the streaming system")
    parser.add_argument(
        "--output-dir",
        default="test_results",
        help="Directory to save test results (default: test_results)"
    )
    parser.add_argument(
        "--test",
        help="Run a specific test by name (default: all tests)"
    )
    
    args = parser.parse_args()
    
    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Run tests
    if args.test:
        # Find the requested test configuration
        config = None
        for c in TEST_CONFIGURATIONS:
            if c["name"] == args.test:
                config = c
                break
        
        if config:
            results = [run_test(config, args.output_dir)]
        else:
            logger.error(f"Test configuration '{args.test}' not found")
            sys.exit(1)
    else:
        # Run all tests
        results = run_all_tests(args.output_dir)
    
    # Generate final report
    report = generate_final_report(results, args.output_dir)
    
    # Log report
    log_final_report(report)
    
    # Exit with success code if all tests passed
    if report["passed_tests"] == report["total_tests"]:
        logger.info("All tests passed!")
        sys.exit(0)
    else:
        logger.warning(f"{report['total_tests'] - report['passed_tests']} tests failed")
        sys.exit(1)

if __name__ == "__main__":
    main() 