"""
Performance optimization module for the streaming system

This module provides functions to optimize the system performance
based on workload characteristics and resource constraints.
"""
import os
import json
import pandas as pd
import logging
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime

from src.streaming.config.settings import (
    BATCH_SIZE,
    THROTTLE_RATE,
    STREAM_RESULTS_DIR
)

logger = logging.getLogger(__name__)

class PerformanceAnalyzer:
    """
    Analyzes system performance and suggests optimizations
    """
    def __init__(self, results_dir: str = STREAM_RESULTS_DIR):
        self.results_dir = results_dir
        self.test_results = []
        self.optimizations = {}
    
    def load_test_results(self, results_file: str):
        """
        Load performance test results from a file
        
        Args:
            results_file: Path to the performance test results JSON file
        """
        try:
            with open(results_file, 'r') as f:
                results = json.load(f)
                self.test_results.append(results)
            logger.info(f"Loaded performance test results from {results_file}")
        except Exception as e:
            logger.error(f"Error loading test results: {str(e)}")
    
    def analyze_throughput(self) -> Dict[str, Any]:
        """
        Analyze throughput metrics and identify bottlenecks
        
        Returns:
            Dict with throughput analysis
        """
        if not self.test_results:
            return {"error": "No test results available for analysis"}
        
        # Extract throughput metrics from all test results
        producer_throughputs = []
        consumer_throughputs = []
        
        for result in self.test_results:
            if "producer" in result and "throughput_records_per_second" in result["producer"]:
                producer_throughputs.append(result["producer"]["throughput_records_per_second"])
            
            if "consumer" in result and "throughput_records_per_second" in result["consumer"]:
                consumer_throughputs.append(result["consumer"]["throughput_records_per_second"])
        
        # Calculate average throughputs
        avg_producer_throughput = sum(producer_throughputs) / len(producer_throughputs) if producer_throughputs else 0
        avg_consumer_throughput = sum(consumer_throughputs) / len(consumer_throughputs) if consumer_throughputs else 0
        
        # Identify bottlenecks
        bottleneck = "consumer" if avg_producer_throughput > avg_consumer_throughput else "producer"
        bottleneck_ratio = avg_producer_throughput / avg_consumer_throughput if avg_consumer_throughput > 0 else float('inf')
        
        # Generate analysis
        analysis = {
            "avg_producer_throughput": avg_producer_throughput,
            "avg_consumer_throughput": avg_consumer_throughput,
            "bottleneck": bottleneck,
            "bottleneck_ratio": bottleneck_ratio,
            "is_balanced": 0.8 <= bottleneck_ratio <= 1.2,  # Within 20% is considered balanced
        }
        
        return analysis
    
    def analyze_latency(self) -> Dict[str, Any]:
        """
        Analyze end-to-end latency
        
        Returns:
            Dict with latency analysis
        """
        if not self.test_results:
            return {"error": "No test results available for analysis"}
        
        # Extract latency metrics from all test results
        avg_latencies = []
        min_latencies = []
        max_latencies = []
        
        for result in self.test_results:
            if "latency" in result:
                if "average_seconds" in result["latency"]:
                    avg_latencies.append(result["latency"]["average_seconds"])
                if "min_seconds" in result["latency"]:
                    min_latencies.append(result["latency"]["min_seconds"])
                if "max_seconds" in result["latency"]:
                    max_latencies.append(result["latency"]["max_seconds"])
        
        # Calculate overall latency statistics
        overall_avg_latency = sum(avg_latencies) / len(avg_latencies) if avg_latencies else 0
        overall_min_latency = min(min_latencies) if min_latencies else 0
        overall_max_latency = max(max_latencies) if max_latencies else 0
        
        # Generate analysis
        analysis = {
            "overall_avg_latency": overall_avg_latency,
            "overall_min_latency": overall_min_latency,
            "overall_max_latency": overall_max_latency,
            "meets_target": overall_avg_latency < 30,  # Target is <30 seconds
            "latency_variability": overall_max_latency - overall_min_latency
        }
        
        return analysis
    
    def suggest_batch_size_optimization(self) -> Dict[str, Any]:
        """
        Suggest optimal batch size based on performance analysis
        
        Returns:
            Dict with batch size optimization suggestion
        """
        throughput_analysis = self.analyze_throughput()
        latency_analysis = self.analyze_latency()
        
        current_batch_size = BATCH_SIZE
        suggested_batch_size = current_batch_size
        
        # Logic for suggesting batch size
        if "error" not in throughput_analysis and "error" not in latency_analysis:
            # If consumer is the bottleneck and latency is high, try smaller batch size
            if (throughput_analysis["bottleneck"] == "consumer" and 
                not latency_analysis["meets_target"]):
                suggested_batch_size = max(100, current_batch_size // 2)
            
            # If producer is the bottleneck and latency is acceptable, try larger batch size
            elif (throughput_analysis["bottleneck"] == "producer" and 
                  latency_analysis["meets_target"]):
                suggested_batch_size = min(2000, current_batch_size * 2)
            
            # If system is balanced, keep current batch size
            else:
                suggested_batch_size = current_batch_size
        
        suggestion = {
            "current_batch_size": current_batch_size,
            "suggested_batch_size": suggested_batch_size,
            "should_change": suggested_batch_size != current_batch_size,
            "explanation": self._get_batch_size_explanation(
                current_batch_size, 
                suggested_batch_size, 
                throughput_analysis, 
                latency_analysis
            )
        }
        
        self.optimizations["batch_size"] = suggestion
        return suggestion
    
    def suggest_throttle_rate_optimization(self) -> Dict[str, Any]:
        """
        Suggest optimal throttle rate based on performance analysis
        
        Returns:
            Dict with throttle rate optimization suggestion
        """
        throughput_analysis = self.analyze_throughput()
        latency_analysis = self.analyze_latency()
        
        current_throttle_rate = THROTTLE_RATE
        suggested_throttle_rate = current_throttle_rate
        
        # Logic for suggesting throttle rate
        if "error" not in throughput_analysis and "error" not in latency_analysis:
            # If consumer is the bottleneck and is significantly slower, increase throttle rate
            if (throughput_analysis["bottleneck"] == "consumer" and 
                throughput_analysis["bottleneck_ratio"] > 1.5):
                suggested_throttle_rate = min(1.0, current_throttle_rate * 2)
            
            # If producer is the bottleneck or system is balanced and latency is good, decrease throttle rate
            elif ((throughput_analysis["bottleneck"] == "producer" or throughput_analysis["is_balanced"]) and 
                  latency_analysis["meets_target"]):
                suggested_throttle_rate = max(0.05, current_throttle_rate / 2)
            
            # Otherwise, keep current throttle rate
            else:
                suggested_throttle_rate = current_throttle_rate
        
        suggestion = {
            "current_throttle_rate": current_throttle_rate,
            "suggested_throttle_rate": suggested_throttle_rate,
            "should_change": abs(suggested_throttle_rate - current_throttle_rate) > 0.01,
            "explanation": self._get_throttle_rate_explanation(
                current_throttle_rate, 
                suggested_throttle_rate, 
                throughput_analysis, 
                latency_analysis
            )
        }
        
        self.optimizations["throttle_rate"] = suggestion
        return suggestion
    
    def _get_batch_size_explanation(
        self, 
        current: int, 
        suggested: int, 
        throughput_analysis: Dict[str, Any], 
        latency_analysis: Dict[str, Any]
    ) -> str:
        """Get explanation for batch size suggestion"""
        if current == suggested:
            return "Current batch size is optimal for the workload"
        
        if suggested > current:
            return (f"Increasing batch size from {current} to {suggested} to improve producer throughput "
                   f"(currently {throughput_analysis.get('avg_producer_throughput', 0):.2f} records/sec)")
        else:
            return (f"Decreasing batch size from {current} to {suggested} to reduce consumer bottleneck "
                   f"and improve latency (currently {latency_analysis.get('overall_avg_latency', 0):.2f} seconds)")
    
    def _get_throttle_rate_explanation(
        self, 
        current: float, 
        suggested: float, 
        throughput_analysis: Dict[str, Any], 
        latency_analysis: Dict[str, Any]
    ) -> str:
        """Get explanation for throttle rate suggestion"""
        if abs(current - suggested) <= 0.01:
            return "Current throttle rate is optimal for the workload"
        
        if suggested > current:
            return (f"Increasing throttle rate from {current:.2f} to {suggested:.2f} seconds to reduce load on consumer "
                   f"(bottleneck ratio: {throughput_analysis.get('bottleneck_ratio', 0):.2f})")
        else:
            return (f"Decreasing throttle rate from {current:.2f} to {suggested:.2f} seconds to increase throughput "
                   f"(current avg: {throughput_analysis.get('avg_producer_throughput', 0):.2f} records/sec)")
    
    def generate_optimization_report(self) -> Dict[str, Any]:
        """
        Generate a comprehensive optimization report
        
        Returns:
            Dict with optimization recommendations
        """
        # Run all analysis if not already done
        throughput_analysis = self.analyze_throughput()
        latency_analysis = self.analyze_latency()
        
        # Generate suggestions if not already done
        if "batch_size" not in self.optimizations:
            self.suggest_batch_size_optimization()
        
        if "throttle_rate" not in self.optimizations:
            self.suggest_throttle_rate_optimization()
        
        # Compile the report
        report = {
            "timestamp": datetime.now().isoformat(),
            "throughput_analysis": throughput_analysis,
            "latency_analysis": latency_analysis,
            "optimizations": self.optimizations,
            "summary": self._generate_summary()
        }
        
        return report
    
    def _generate_summary(self) -> str:
        """Generate a summary of optimization recommendations"""
        batch_size_opt = self.optimizations.get("batch_size", {})
        throttle_rate_opt = self.optimizations.get("throttle_rate", {})
        
        summary_parts = []
        
        if batch_size_opt.get("should_change", False):
            summary_parts.append(batch_size_opt.get("explanation", ""))
        
        if throttle_rate_opt.get("should_change", False):
            summary_parts.append(throttle_rate_opt.get("explanation", ""))
        
        if not summary_parts:
            summary_parts.append("Current configuration is optimal for the workload")
        
        return " ".join(summary_parts)
    
    def save_report(self, filename: str = "optimization_report.json"):
        """
        Save the optimization report to a file
        
        Args:
            filename: Output filename
        """
        report = self.generate_optimization_report()
        
        try:
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            with open(filename, 'w') as f:
                json.dump(report, f, indent=2)
            logger.info(f"Saved optimization report to {filename}")
        except Exception as e:
            logger.error(f"Error saving optimization report: {str(e)}")

def optimize_configuration(test_results_file: str) -> Dict[str, Any]:
    """
    Analyze test results and suggest optimized configuration
    
    Args:
        test_results_file: Path to test results JSON file
        
    Returns:
        Dict with optimized configuration parameters
    """
    analyzer = PerformanceAnalyzer()
    analyzer.load_test_results(test_results_file)
    
    # Generate optimization report
    report = analyzer.generate_optimization_report()
    
    # Extract optimized configuration
    optimized_config = {
        "batch_size": report["optimizations"]["batch_size"]["suggested_batch_size"],
        "throttle_rate": report["optimizations"]["throttle_rate"]["suggested_throttle_rate"]
    }
    
    return optimized_config

def apply_optimized_configuration(config: Dict[str, Any]) -> bool:
    """
    Apply optimized configuration to the system
    
    Args:
        config: Dict with configuration parameters to apply
        
    Returns:
        bool: True if configuration was applied successfully
    """
    try:
        # Here we would update the configuration file or environment variables
        # For now, we'll just print the new configuration
        logger.info(f"Applying optimized configuration: {config}")
        
        # In a real implementation, we would update the actual configuration
        # For example:
        # with open('.env', 'w') as f:
        #     f.write(f"BATCH_SIZE={config['batch_size']}\n")
        #     f.write(f"THROTTLE_RATE={config['throttle_rate']}\n")
        
        return True
    except Exception as e:
        logger.error(f"Error applying optimized configuration: {str(e)}")
        return False 