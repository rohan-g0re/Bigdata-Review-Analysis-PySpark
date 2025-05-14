import pandas as pd
import streamlit as st
from pyspark.sql.functions import col, avg, round as spark_round, to_date, count, date_format, sum as spark_sum
from src.utils.data_processing import truncate_text
from src.tabs.game_tab.sentiment_analysis import load_sentiment_model, analyze_sentiment_batch, get_overall_sentiment, get_extreme_reviews

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
        
        # Check if any records were found after date filtering
        date_filtered_count = game_df.count()
        if date_filtered_count == 0:
            return None
        
        # Get a list of available languages for this game in the selected time period
        available_languages = game_df.select("language").distinct().collect()
        available_languages = [row["language"] for row in available_languages]
        
        # Calculate statistics on the date-filtered data
        review_count = date_filtered_count
        
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
        
        # Get received_for_free statistics for pie chart
        received_free_stats = game_df.groupBy("received_for_free") \
            .count() \
            .orderBy("received_for_free") \
            .collect()
            
        # Convert to pandas DataFrame for pie chart
        received_free_data = pd.DataFrame([
            {
                "category": "Free" if row["received_for_free"] else "Purchased",
                "count": row["count"]
            } for row in received_free_stats
        ])
        
        # Get early access review count
        early_access_count = game_df.filter(col("written_during_early_access") == True).count()
        
        # Get ALL reviews for sentiment analysis
        with st.spinner("Performing sentiment analysis on reviews..."):
            # We need the raw review text for sentiment analysis
            all_reviews = game_df.select(
                "author_steamid", "review", "votes_up", "votes_funny", "timestamp_created", 
                "author_playtime_at_review", "language"
            ).collect()
            
            # Convert to pandas DataFrame with all columns
            all_reviews_df = pd.DataFrame([
                {
                    "author_id": row["author_steamid"],
                    "review_text": row["review"],  # Full text for sentiment analysis
                    "review_text_truncated": truncate_text(row["review"]),  # Truncated for display
                    "votes_up": row["votes_up"],
                    "votes_funny": row["votes_funny"],
                    "date": row["timestamp_created"].strftime("%Y-%m-%d"),
                    "playtime_hrs": round(row["author_playtime_at_review"] / 60, 1) if row["author_playtime_at_review"] else 0,
                    "language": row["language"]
                } for row in all_reviews
            ])
            
            # Load sentiment model (this is cached so it won't reload every time)
            model, tokenizer = load_sentiment_model()
            
            # Determine the optimal sample size for sentiment analysis
            max_reviews_to_analyze = 1000  # Cap at 1000 for performance
            sample_size = min(len(all_reviews_df), max_reviews_to_analyze)
            
            # Create a progress bar
            progress_text = f"Analyzing sentiment of {sample_size} reviews"
            progress_bar = st.progress(0, text=progress_text)
            
            # Select a representative sample - include English reviews first if available
            if 'english' in all_reviews_df['language'].values:
                # Get English reviews first (they usually have better sentiment analysis results)
                english_reviews = all_reviews_df[all_reviews_df['language'] == 'english']
                english_sample_size = min(len(english_reviews), int(sample_size * 0.7))  # Use up to 70% English reviews
                
                # Get other languages for the rest of the sample
                non_english = all_reviews_df[all_reviews_df['language'] != 'english']
                other_sample_size = sample_size - english_sample_size
                
                # Combine the samples
                review_sample = pd.concat([
                    english_reviews.sample(english_sample_size) if len(english_reviews) > english_sample_size else english_reviews,
                    non_english.sample(other_sample_size) if len(non_english) > other_sample_size else non_english
                ])
            else:
                # If no English reviews, just take a random sample
                review_sample = all_reviews_df.sample(sample_size) if len(all_reviews_df) > sample_size else all_reviews_df
            
            # Extract review texts for analysis
            review_texts = review_sample["review_text"].tolist()
            
            # Analyze sentiment in batches with progress tracking
            batch_size = 32
            num_batches = (len(review_texts) + batch_size - 1) // batch_size  # Ceiling division
            
            sentiment_results = []
            for i in range(0, len(review_texts), batch_size):
                # Get the current batch
                batch_texts = review_texts[i:i+batch_size]
                
                # Analyze the batch
                batch_results = analyze_sentiment_batch(batch_texts, model, tokenizer, batch_size=batch_size)
                sentiment_results.extend(batch_results)
                
                # Update progress
                progress = min(1.0, (i + len(batch_texts)) / len(review_texts))
                progress_bar.progress(progress, text=f"{progress_text} ({i + len(batch_texts)}/{len(review_texts)})")
            
            # Clear the progress bar
            progress_bar.empty()
            
            # Calculate overall sentiment
            overall_sentiment = get_overall_sentiment(sentiment_results)
            
            # Get top positive and negative reviews
            top_positive = get_extreme_reviews(review_sample, sentiment_results, count=10, sentiment_type="positive")
            top_negative = get_extreme_reviews(review_sample, sentiment_results, count=10, sentiment_type="negative")
            
            # Use truncated text for display but keep other columns
            top_positive["review_text"] = top_positive["review_text_truncated"]
            top_negative["review_text"] = top_negative["review_text_truncated"]
        
        # Get top upvoted reviews for traditional display
        top_upvoted_reviews = game_df.select(
            "author_steamid", "review", "votes_up", "timestamp_created", "author_playtime_at_review", "language"
        ).orderBy(col("votes_up").desc()).limit(100).collect()
        
        # Convert to pandas DataFrame
        all_upvoted_df = pd.DataFrame([
            {
                "author_id": row["author_steamid"],
                "review_text": truncate_text(row["review"]),
                "votes_up": row["votes_up"],
                "date": row["timestamp_created"].strftime("%Y-%m-%d"),
                "playtime_hrs": round(row["author_playtime_at_review"] / 60, 1) if row["author_playtime_at_review"] else 0,
                "language": row["language"]
            } for row in top_upvoted_reviews
        ])
        
        # Get top funny reviews
        top_funny_reviews = game_df.select(
            "author_steamid", "review", "votes_funny", "timestamp_created", "author_playtime_at_review", "language"
        ).orderBy(col("votes_funny").desc()).limit(100).collect()
        
        # Convert to pandas DataFrame
        all_funny_df = pd.DataFrame([
            {
                "author_id": row["author_steamid"],
                "review_text": truncate_text(row["review"]),
                "votes_funny": row["votes_funny"],
                "date": row["timestamp_created"].strftime("%Y-%m-%d"),
                "playtime_hrs": round(row["author_playtime_at_review"] / 60, 1) if row["author_playtime_at_review"] else 0,
                "language": row["language"]
            } for row in top_funny_reviews
        ])
        
        # Return the information
        return {
            "game_name": game_name,
            "total_reviews": review_count,
            "available_languages": available_languages,
            "avg_playtime": avg_playtime if avg_playtime is not None else 0,
            "time_series_data": time_series_data,
            "all_upvoted_reviews": all_upvoted_df,
            "all_funny_reviews": all_funny_df,
            "received_free_data": received_free_data,
            "early_access_count": early_access_count,
            # New sentiment analysis data
            "sentiment_score": overall_sentiment,
            "top_positive_reviews": top_positive,
            "top_negative_reviews": top_negative,
            "sentiment_analyzed_count": len(review_texts)
        }
        
    except Exception as e:
        st.error(f"Error during game analysis: {e}")
        return None 