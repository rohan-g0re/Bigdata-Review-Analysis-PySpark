"""
Utilities for accessing streaming data in the dashboard
"""
import os
import time
import pandas as pd
from datetime import datetime
from typing import Dict, Any, Tuple, List, Optional

from src.streaming.config.settings import STREAM_RESULTS_DIR

class StreamDataManager:
    """
    Class to manage access to streaming data for the dashboard
    """
    def __init__(self, results_dir: str = STREAM_RESULTS_DIR):
        self.results_dir = results_dir
        self.last_refresh_time = None
        self.data_cache = {}
        self.file_timestamps = {}
    
    def get_data_freshness(self) -> Dict[str, Any]:
        """
        Get information about data freshness
        
        Returns:
            Dict with freshness information
        """
        freshness_info = {
            "last_refresh": self.last_refresh_time,
            "files": self.file_timestamps,
            "has_data": len(self.data_cache) > 0
        }
        
        # Add overall freshness status
        if not freshness_info["has_data"]:
            freshness_info["status"] = "no_data"
        elif not self.last_refresh_time:
            freshness_info["status"] = "not_refreshed"
        else:
            seconds_since_refresh = (datetime.now() - self.last_refresh_time).total_seconds()
            if seconds_since_refresh < 30:
                freshness_info["status"] = "fresh"
            elif seconds_since_refresh < 120:
                freshness_info["status"] = "recent"
            else:
                freshness_info["status"] = "stale"
                
        return freshness_info
    
    def load_streaming_data(self, force_refresh: bool = True) -> Tuple[
        Optional[pd.DataFrame], 
        Optional[pd.DataFrame], 
        Optional[pd.DataFrame], 
        Optional[pd.DataFrame]
    ]:
        """
        Load streaming data from the results directory with caching
        
        Args:
            force_refresh: Whether to force reload data from disk
            
        Returns:
            tuple: (top_games_df, sentiment_df, time_df, recent_reviews_df)
        """
        # Check if we need to refresh the data
        need_refresh = force_refresh or not self.data_cache
        
        if not need_refresh and self.file_timestamps:
            # Check if any files have been modified
            for file_path, last_mod in self.file_timestamps.items():
                if os.path.exists(file_path):
                    current_mod = os.path.getmtime(file_path)
                    if current_mod > last_mod:
                        need_refresh = True
                        break
        
        if not need_refresh:
            # Return cached data
            return (
                self.data_cache.get("top_games"),
                self.data_cache.get("sentiment"),
                self.data_cache.get("time"),
                self.data_cache.get("reviews")
            )
        
        # Load fresh data
        top_games_df = None
        sentiment_df = None
        time_df = None
        recent_reviews_df = None
        
        try:
            # Update file timestamps
            self.file_timestamps = {}
            
            # Load top games data
            top_games_path = os.path.join(self.results_dir, 'top_games.csv')
            if os.path.exists(top_games_path):
                top_games_df = pd.read_csv(top_games_path)
                self.file_timestamps[top_games_path] = os.path.getmtime(top_games_path)
                
            # Load sentiment analysis data
            sentiment_path = os.path.join(self.results_dir, 'sentiment_analysis.csv')
            if os.path.exists(sentiment_path):
                sentiment_df = pd.read_csv(sentiment_path)
                self.file_timestamps[sentiment_path] = os.path.getmtime(sentiment_path)
                
            # Load time distribution data
            time_path = os.path.join(self.results_dir, 'time_distribution.csv')
            if os.path.exists(time_path):
                time_df = pd.read_csv(time_path)
                self.file_timestamps[time_path] = os.path.getmtime(time_path)
                
            # Load recent reviews data
            reviews_path = os.path.join(self.results_dir, 'recent_reviews.csv')
            if os.path.exists(reviews_path):
                recent_reviews_df = pd.read_csv(reviews_path)
                self.file_timestamps[reviews_path] = os.path.getmtime(reviews_path)
            
            # Update cache
            self.data_cache = {
                "top_games": top_games_df,
                "sentiment": sentiment_df,
                "time": time_df,
                "reviews": recent_reviews_df
            }
            
            # Update refresh time
            self.last_refresh_time = datetime.now()
            
        except Exception as e:
            print(f"Error loading streaming data: {str(e)}")
        
        return top_games_df, sentiment_df, time_df, recent_reviews_df
    
    def get_top_games(self, limit: int = 10, force_refresh: bool = False) -> Optional[pd.DataFrame]:
        """
        Get top games from streaming data
        
        Args:
            limit: Number of top games to return
            force_refresh: Whether to force reload data
            
        Returns:
            DataFrame with top games or None
        """
        top_games_df, _, _, _ = self.load_streaming_data(force_refresh)
        
        if top_games_df is not None and not top_games_df.empty:
            return top_games_df.sort_values('review_count', ascending=False).head(limit)
        
        return None
    
    def get_sentiment_analysis(self, limit: int = 20, force_refresh: bool = False) -> Optional[pd.DataFrame]:
        """
        Get sentiment analysis data
        
        Args:
            limit: Number of games to include
            force_refresh: Whether to force reload data
            
        Returns:
            DataFrame with sentiment data or None
        """
        _, sentiment_df, _, _ = self.load_streaming_data(force_refresh)
        
        if sentiment_df is not None and not sentiment_df.empty:
            return sentiment_df.sort_values('review_count', ascending=False).head(limit)
        
        return None
    
    def get_time_distribution(self, force_refresh: bool = False) -> Optional[pd.DataFrame]:
        """
        Get time distribution data
        
        Args:
            force_refresh: Whether to force reload data
            
        Returns:
            DataFrame with time distribution or None
        """
        _, _, time_df, _ = self.load_streaming_data(force_refresh)
        
        if time_df is not None and not time_df.empty:
            # Ensure datetime format for the hour column
            if 'hour' in time_df.columns:
                try:
                    time_df['hour'] = pd.to_datetime(time_df['hour'])
                except:
                    pass
            return time_df.sort_values('hour')
        
        return None
    
    def get_recent_reviews(self, limit: int = 5, force_refresh: bool = False) -> Optional[pd.DataFrame]:
        """
        Get recent reviews data
        
        Args:
            limit: Number of recent reviews to return
            force_refresh: Whether to force reload data
            
        Returns:
            DataFrame with recent reviews or None
        """
        _, _, _, recent_reviews_df = self.load_streaming_data(force_refresh)
        
        if recent_reviews_df is not None and not recent_reviews_df.empty:
            return recent_reviews_df.sort_values('timestamp', ascending=False).head(limit)
        
        return None

# Singleton instance for use throughout the app
stream_data_manager = StreamDataManager() 