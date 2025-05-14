import pandas as pd
import streamlit as st
from pyspark.sql.functions import col, avg, round as spark_round, to_date, count, date_format
from src.utils.data_processing import truncate_text

def get_game_info(spark, parquet_dir, game_name, start_date=None, end_date=None):
    """
    Get specific information about a game from the Parquet files.
    
    Parameters:
    - spark: SparkSession object
    - parquet_dir: Path to the directory containing Parquet files
    - game_name: Name of the game to search for
    - start_date: Optional start date filter (datetime)
    - end_date: Optional end date filter (datetime)
    
    Returns:
    - Dictionary with game information
    """
    try:
        # Load all Parquet files from the directory
        df = spark.read.parquet(parquet_dir)
        
        # Filter for the specific game
        game_df = df.filter(col("game") == game_name)
        
        # Check if we found any reviews for this game
        initial_count = game_df.count()
        if initial_count == 0:
            return None
            
        # Add a date column for filtering and analysis
        game_df = game_df.withColumn("review_date", to_date(col("timestamp_created")))
        
        # Apply date filtering if provided
        if start_date and end_date:
            # Convert Python datetime to string format YYYY-MM-DD for Spark SQL
            start_str = start_date.strftime("%Y-%m-%d")
            end_str = end_date.strftime("%Y-%m-%d")
            
            # Filter by date range
            game_df = game_df.filter((col("review_date") >= start_str) & (col("review_date") <= end_str))
        
        # Check if any records were found after filtering
        review_count = game_df.count()
        if review_count == 0:
            return None
        
        # Calculate average playtime at review, handling null values
        avg_playtime = game_df.select(
            spark_round(avg("author_playtime_at_review"), 2)
        ).collect()[0][0]
        
        # Get time series data for reviews by day
        time_series = game_df.groupBy("review_date") \
            .count() \
            .orderBy("review_date") \
            .collect()
        
        # Convert to pandas DataFrame for plotting
        time_series_data = pd.DataFrame(
            [(row["review_date"].strftime("%Y-%m-%d"), row["count"]) for row in time_series],
            columns=["date", "count"]
        )
        
        # Get top 10 reviews with most upvotes
        top_upvoted_reviews = game_df.select(
            "author_steamid", "review", "votes_up", "timestamp_created", "author_playtime_at_review"
        ).orderBy(col("votes_up").desc()).limit(10).collect()
        
        # Convert to pandas DataFrame
        top_upvoted_df = pd.DataFrame([
            {
                "author_id": row["author_steamid"],
                "review_text": truncate_text(row["review"]),
                "votes_up": row["votes_up"],
                "date": row["timestamp_created"].strftime("%Y-%m-%d"),
                "playtime_hrs": round(row["author_playtime_at_review"] / 60, 1) if row["author_playtime_at_review"] else 0
            } for row in top_upvoted_reviews
        ])
        
        # Get top 10 reviews with most funny votes
        top_funny_reviews = game_df.select(
            "author_steamid", "review", "votes_funny", "timestamp_created", "author_playtime_at_review"
        ).orderBy(col("votes_funny").desc()).limit(10).collect()
        
        # Convert to pandas DataFrame
        top_funny_df = pd.DataFrame([
            {
                "author_id": row["author_steamid"],
                "review_text": truncate_text(row["review"]),
                "votes_funny": row["votes_funny"],
                "date": row["timestamp_created"].strftime("%Y-%m-%d"),
                "playtime_hrs": round(row["author_playtime_at_review"] / 60, 1) if row["author_playtime_at_review"] else 0
            } for row in top_funny_reviews
        ])
        
        # Return the information
        return {
            "game_name": game_name,
            "total_reviews": review_count,
            "avg_playtime": avg_playtime if avg_playtime is not None else 0,
            "time_series_data": time_series_data,
            "top_upvoted_reviews": top_upvoted_df,
            "top_funny_reviews": top_funny_df
        }
        
    except Exception as e:
        st.error(f"Error during game analysis: {e}")
        return None 