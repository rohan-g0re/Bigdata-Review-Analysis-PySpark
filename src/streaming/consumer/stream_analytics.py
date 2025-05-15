"""
Analytics for streaming data

This module contains classes for analyzing streaming review data.
"""
import os
import csv
import json
import time
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

class StreamAnalytics:
    """
    Analytics for streaming data
    """
    def __init__(self, results_dir: str):
        """
        Initialize the analytics component
        
        Args:
            results_dir: Directory to save results
        """
        self.results_dir = results_dir
        self.recent_reviews = []
        self.game_counts = {}
        self.sentiment_totals = {'positive': 0, 'negative': 0, 'neutral': 0}
        self.reviews_by_language = {}
        self.hourly_stats = {}
        self.last_save_time = time.time()
        self.save_interval = 3  # Save every 3 seconds
    
    def initialize(self) -> bool:
        """
        Initialize the analytics component
        
        Returns:
            bool: True if initialization was successful
        """
        try:
            # Create the results directory if it doesn't exist
            os.makedirs(self.results_dir, exist_ok=True)
            
            # Initialize data structures
            self.recent_reviews = []
            self.game_counts = {}
            self.sentiment_totals = {'positive': 0, 'negative': 0, 'neutral': 0}
            self.reviews_by_language = {}
            self.hourly_stats = {}
            
            return True
        except Exception as e:
            logger.error(f"Error initializing analytics: {str(e)}")
            return False
    
    def update(self, review: Dict[str, Any]):
        """
        Update analytics with a new review
        
        Args:
            review: Review dictionary
        """
        try:
            # Add to recent reviews (keep only 100 most recent)
            self.recent_reviews.append(review)
            if len(self.recent_reviews) > 100:
                self.recent_reviews = self.recent_reviews[-100:]
            
            # Count games
            app_id = review.get('app_id')
            if app_id:
                self.game_counts[app_id] = self.game_counts.get(app_id, 0) + 1
            
            # Sentiment analysis
            sentiment = self._get_sentiment(review)
            self.sentiment_totals[sentiment] += 1
            
            # Language stats
            language = review.get('language', 'unknown')
            if language not in self.reviews_by_language:
                self.reviews_by_language[language] = 0
            self.reviews_by_language[language] += 1
            
            # Hourly stats
            timestamp = review.get('timestamp_created')
            if timestamp:
                hour = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:00:00')
                if hour not in self.hourly_stats:
                    self.hourly_stats[hour] = 0
                self.hourly_stats[hour] += 1
            
            # Save results periodically
            now = time.time()
            if now - self.last_save_time >= self.save_interval:
                self.save_results()
                self.last_save_time = now
                
        except Exception as e:
            logger.error(f"Error updating analytics: {str(e)}")
    
    def process_review(self, review: Dict[str, Any]):
        """
        Process a review (alias for update)
        
        Args:
            review: Review dictionary
        """
        self.update(review)
    
    def _get_sentiment(self, review: Dict[str, Any]) -> str:
        """
        Get sentiment from a review
        
        Args:
            review: Review dictionary
            
        Returns:
            str: 'positive', 'negative', or 'neutral'
        """
        # Use voted_up field if available
        if 'voted_up' in review:
            return 'positive' if review['voted_up'] else 'negative'
        
        # Fall back to sentiment analysis if available
        if 'sentiment_score' in review:
            score = review['sentiment_score']
            if score > 0.1:
                return 'positive'
            elif score < -0.1:
                return 'negative'
            else:
                return 'neutral'
        
        # Default to neutral
        return 'neutral'
    
    def save_results(self):
        """Save analytics results to files"""
        try:
            # Create results directory if it doesn't exist
            os.makedirs(self.results_dir, exist_ok=True)
            
            # Save recent reviews
            self._save_recent_reviews()
            
            # Save game counts
            self._save_game_counts()
            
            # Save sentiment totals
            self._save_sentiment_totals()
            
            # Save language stats
            self._save_language_stats()
            
            # Save hourly stats
            self._save_hourly_stats()
            
            logger.debug("Saved analytics results")
            
        except Exception as e:
            logger.error(f"Error saving analytics results: {str(e)}")
    
    def _save_recent_reviews(self):
        """Save recent reviews to CSV file"""
        try:
            file_path = os.path.join(self.results_dir, 'recent_reviews.csv')
            
            with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
                if not self.recent_reviews:
                    return
                
                # Get all possible columns from the reviews
                all_columns = set()
                for review in self.recent_reviews:
                    all_columns.update(review.keys())
                
                # Prioritize important columns
                first_columns = ['app_id', 'author', 'review', 'voted_up', 'timestamp_created']
                # Remove these from all_columns to avoid duplication
                remaining_columns = [col for col in all_columns if col not in first_columns]
                # Create the final column list
                columns = first_columns + sorted(remaining_columns)
                
                writer = csv.DictWriter(csvfile, fieldnames=columns)
                writer.writeheader()
                
                for review in self.recent_reviews:
                    # Convert timestamps to readable format
                    if 'timestamp_created' in review and isinstance(review['timestamp_created'], int):
                        review = review.copy()  # Don't modify the original
                        review['timestamp_created'] = datetime.fromtimestamp(
                            review['timestamp_created']
                        ).strftime('%Y-%m-%d %H:%M:%S')
                    
                    writer.writerow(review)
        except Exception as e:
            logger.error(f"Error saving recent reviews: {str(e)}")
    
    def _save_game_counts(self):
        """Save game counts to CSV file"""
        try:
            file_path = os.path.join(self.results_dir, 'game_counts.csv')
            
            with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['app_id', 'count'])
                
                # Sort by count (descending)
                sorted_games = sorted(
                    self.game_counts.items(),
                    key=lambda x: x[1],
                    reverse=True
                )
                
                for app_id, count in sorted_games:
                    writer.writerow([app_id, count])
        except Exception as e:
            logger.error(f"Error saving game counts: {str(e)}")
    
    def _save_sentiment_totals(self):
        """Save sentiment totals to CSV file"""
        try:
            file_path = os.path.join(self.results_dir, 'sentiment_totals.csv')
            
            with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['sentiment', 'count'])
                
                for sentiment, count in self.sentiment_totals.items():
                    writer.writerow([sentiment, count])
        except Exception as e:
            logger.error(f"Error saving sentiment totals: {str(e)}")
    
    def _save_language_stats(self):
        """Save language stats to CSV file"""
        try:
            file_path = os.path.join(self.results_dir, 'language_stats.csv')
            
            with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['language', 'count'])
                
                # Sort by count (descending)
                sorted_languages = sorted(
                    self.reviews_by_language.items(),
                    key=lambda x: x[1],
                    reverse=True
                )
                
                for language, count in sorted_languages:
                    writer.writerow([language, count])
        except Exception as e:
            logger.error(f"Error saving language stats: {str(e)}")
    
    def _save_hourly_stats(self):
        """Save hourly stats to CSV file"""
        try:
            file_path = os.path.join(self.results_dir, 'hourly_stats.csv')
            
            with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['hour', 'count'])
                
                # Sort by hour
                sorted_hours = sorted(self.hourly_stats.items())
                
                for hour, count in sorted_hours:
                    writer.writerow([hour, count])
        except Exception as e:
            logger.error(f"Error saving hourly stats: {str(e)}")
    
    def get_top_games(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get the top games by review count
        
        Args:
            limit: Maximum number of games to return
            
        Returns:
            List of game info dictionaries
        """
        # Sort by count (descending)
        sorted_games = sorted(
            self.game_counts.items(),
            key=lambda x: x[1],
            reverse=True
        )
        
        # Limit the number of games
        top_games = sorted_games[:limit]
        
        # Convert to list of dictionaries
        return [{'app_id': app_id, 'count': count} for app_id, count in top_games]
    
    def get_sentiment_stats(self) -> Dict[str, Any]:
        """
        Get sentiment statistics
        
        Returns:
            Dict with sentiment statistics
        """
        total = sum(self.sentiment_totals.values())
        if total == 0:
            return {
                'total': 0,
                'positive_pct': 0,
                'negative_pct': 0,
                'neutral_pct': 0,
                'sentiment_totals': self.sentiment_totals.copy()
            }
        
        return {
            'total': total,
            'positive_pct': (self.sentiment_totals['positive'] / total) * 100,
            'negative_pct': (self.sentiment_totals['negative'] / total) * 100,
            'neutral_pct': (self.sentiment_totals['neutral'] / total) * 100,
            'sentiment_totals': self.sentiment_totals.copy()
        }
    
    def get_language_stats(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get language statistics
        
        Args:
            limit: Maximum number of languages to return
            
        Returns:
            List of language statistics dictionaries
        """
        # Sort by count (descending)
        sorted_languages = sorted(
            self.reviews_by_language.items(),
            key=lambda x: x[1],
            reverse=True
        )
        
        # Limit the number of languages
        top_languages = sorted_languages[:limit]
        
        # Convert to list of dictionaries
        return [{'language': lang, 'count': count} for lang, count in top_languages] 