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
    def __init__(self, results_dir: str = STREAM_RESULTS_DIR, debug_mode: bool = False):
        self.results_dir = results_dir
        self.last_refresh_time = None
        self.data_cache = {}
        self.file_timestamps = {}
        self.debug_mode = debug_mode
    
    def _debug_print(self, message: str) -> None:
        """Helper method for debug logging"""
        if self.debug_mode:
            print(message)
    
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
            if os.path.exists(top_games_path) and os.path.getsize(top_games_path) > 0:
                try:
                    top_games_df = pd.read_csv(top_games_path, encoding='utf-8')
                    self.file_timestamps[top_games_path] = os.path.getmtime(top_games_path)
                except UnicodeDecodeError:
                    # Try with error handling
                    try:
                        top_games_df = pd.read_csv(top_games_path, encoding='utf-8', encoding_errors='replace')
                        self.file_timestamps[top_games_path] = os.path.getmtime(top_games_path)
                    except Exception as e:
                        self._debug_print(f"Error reading {top_games_path}: {str(e)}")
                except Exception as e:
                    self._debug_print(f"Error reading {top_games_path}: {str(e)}")
                
            # Load sentiment analysis data
            sentiment_path = os.path.join(self.results_dir, 'sentiment_analysis.csv')
            if os.path.exists(sentiment_path) and os.path.getsize(sentiment_path) > 0:
                try:
                    sentiment_df = pd.read_csv(sentiment_path, encoding='utf-8')
                    self.file_timestamps[sentiment_path] = os.path.getmtime(sentiment_path)
                except UnicodeDecodeError:
                    try:
                        sentiment_df = pd.read_csv(sentiment_path, encoding='utf-8', encoding_errors='replace')
                        self.file_timestamps[sentiment_path] = os.path.getmtime(sentiment_path)
                    except Exception as e:
                        self._debug_print(f"Error reading {sentiment_path}: {str(e)}")
                except Exception as e:
                    self._debug_print(f"Error reading {sentiment_path}: {str(e)}")
                
            # Load time distribution data
            time_path = os.path.join(self.results_dir, 'time_distribution.csv')
            if os.path.exists(time_path) and os.path.getsize(time_path) > 0:
                try:
                    time_df = pd.read_csv(time_path, encoding='utf-8')
                    self.file_timestamps[time_path] = os.path.getmtime(time_path)
                except UnicodeDecodeError:
                    try:
                        time_df = pd.read_csv(time_path, encoding='utf-8', encoding_errors='replace')
                        self.file_timestamps[time_path] = os.path.getmtime(time_path)
                    except Exception as e:
                        self._debug_print(f"Error reading {time_path}: {str(e)}")
                except Exception as e:
                    self._debug_print(f"Error reading {time_path}: {str(e)}")
                
            # Load recent reviews data
            reviews_path = os.path.join(self.results_dir, 'recent_reviews.csv')
            if os.path.exists(reviews_path) and os.path.getsize(reviews_path) > 0:
                try:
                    recent_reviews_df = pd.read_csv(reviews_path, encoding='utf-8')
                    self.file_timestamps[reviews_path] = os.path.getmtime(reviews_path)
                except UnicodeDecodeError:
                    try:
                        recent_reviews_df = pd.read_csv(reviews_path, encoding='utf-8', encoding_errors='replace')
                        self.file_timestamps[reviews_path] = os.path.getmtime(reviews_path)
                    except Exception as e:
                        self._debug_print(f"Error reading {reviews_path}: {str(e)}")
                except Exception as e:
                    self._debug_print(f"Error reading {reviews_path}: {str(e)}")
            
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
            self._debug_print(f"Error loading streaming data: {str(e)}")
        
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
            # Debug: print the columns to help troubleshoot
            self._debug_print(f"Top games DataFrame columns: {list(top_games_df.columns)}")
            
            # Check if the expected column exists
            sort_column = 'review_count'
            if sort_column not in top_games_df.columns:
                # Look for alternative column names
                if 'count' in top_games_df.columns:
                    sort_column = 'count'
                elif 'game_count' in top_games_df.columns:
                    sort_column = 'game_count'
                else:
                    # If no suitable column found, just return the data unsorted
                    self._debug_print(f"Warning: Could not find a suitable column to sort by. Available columns: {list(top_games_df.columns)}")
                    return top_games_df.head(limit)
            
            # Sort by the identified column
            return top_games_df.sort_values(sort_column, ascending=False).head(limit)
        
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
            # Debug: print the columns to help troubleshoot
            self._debug_print(f"Sentiment DataFrame columns: {list(sentiment_df.columns)}")
            
            # Check if the expected column exists
            sort_column = 'review_count'
            if sort_column not in sentiment_df.columns:
                # Look for alternative column names
                if 'count' in sentiment_df.columns:
                    sort_column = 'count'
                elif 'game_count' in sentiment_df.columns:
                    sort_column = 'game_count'
                else:
                    # If no suitable column found, just return the data unsorted
                    self._debug_print(f"Warning: Could not find a suitable column to sort by in sentiment data. Available columns: {list(sentiment_df.columns)}")
                    return sentiment_df.head(limit)
            
            # Sort by the identified column
            return sentiment_df.sort_values(sort_column, ascending=False).head(limit)
        
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
            # Debug: print the columns to help troubleshoot
            self._debug_print(f"Time distribution DataFrame columns: {list(time_df.columns)}")
            
            # Ensure datetime format for the hour column if it exists
            if 'hour' in time_df.columns:
                try:
                    time_df['hour'] = pd.to_datetime(time_df['hour'])
                    return time_df.sort_values('hour')
                except Exception as e:
                    self._debug_print(f"Error converting 'hour' to datetime: {str(e)}")
                    return time_df
            else:
                self._debug_print(f"Warning: 'hour' column not found in time distribution data. Available columns: {list(time_df.columns)}")
                return time_df
        
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
            # Debug: print the columns to help troubleshoot
            self._debug_print(f"Recent reviews DataFrame columns: {list(recent_reviews_df.columns)}")
            
            # Check if the expected column exists
            sort_column = 'timestamp'
            if sort_column not in recent_reviews_df.columns:
                # Look for alternative column names
                if 'timestamp_created' in recent_reviews_df.columns:
                    sort_column = 'timestamp_created'
                elif 'date' in recent_reviews_df.columns:
                    sort_column = 'date'
                else:
                    # If no suitable column found, just return the data unsorted
                    self._debug_print(f"Warning: Could not find a suitable column to sort by in recent reviews. Available columns: {list(recent_reviews_df.columns)}")
                    return recent_reviews_df.head(limit)
            
            # Sort by the identified column
            return recent_reviews_df.sort_values(sort_column, ascending=False).head(limit)
        
        return None

# Singleton instance for use throughout the app
stream_data_manager = StreamDataManager(debug_mode=False) 