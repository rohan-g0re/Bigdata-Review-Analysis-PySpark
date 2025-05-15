import pandas as pd
from pyspark.sql.functions import col, to_date, count, sum as spark_sum
from pyspark.sql import Window
import pyspark.sql.functions as F

def get_top_k_games_by_reviews(spark, parquet_dir, k=10, start_date=None, end_date=None):
    """
    Find the top k games with the most reviews from a directory of Parquet files,
    grouping by the 'game' column, and display results with a bar chart.
    
    Parameters:
    - spark: SparkSession object
    - parquet_dir: Path to the directory containing Parquet files
    - k: Number of top games to return (default: 10)
    - start_date: Optional start date filter (datetime)
    - end_date: Optional end date filter (datetime)
    
    Returns:
    - pandas DataFrame with the top k games
    """
    try:
        # Load all Parquet files from the directory
        df = spark.read.parquet(parquet_dir)
        
        # Verify if 'game' column exists
        if "game" not in df.columns:
            raise ValueError("Column 'game' not found in the dataset. Available columns: " + ", ".join(df.columns))
        
        # Add a date column for filtering and analysis
        df = df.withColumn("review_date", to_date(col("timestamp_created")))
        
        # Apply date filtering if provided
        if start_date and end_date:
            # Convert Python datetime to string format YYYY-MM-DD for Spark SQL
            start_str = start_date.strftime("%Y-%m-%d")
            end_str = end_date.strftime("%Y-%m-%d")
            
            # Filter by date range
            df = df.filter((col("review_date") >= start_str) & (col("review_date") <= end_str))
        
        # Group by 'game', count reviews, and get top k
        top_games_spark = df.groupBy("game") \
                     .count() \
                     .orderBy(col("count").desc()) \
                     .limit(k)
        
        # Convert to pandas manually using collect() instead of toPandas()
        top_games_data = top_games_spark.collect()
        top_games = pd.DataFrame([(row["game"], row["count"]) for row in top_games_data], 
                                 columns=["game", "count"])
        
        return top_games
    except Exception as e:
        raise e 

def get_top_authors_by_review_count(spark, parquet_dir, k=10, start_date=None, end_date=None):
    """
    Find the top k authors with the most reviews from a directory of Parquet files.
    
    Parameters:
    - spark: SparkSession object
    - parquet_dir: Path to the directory containing Parquet files
    - k: Number of top authors to return (default: 10)
    - start_date: Optional start date filter (datetime)
    - end_date: Optional end date filter (datetime)
    
    Returns:
    - pandas DataFrame with the top k authors
    """
    try:
        # Load all Parquet files from the directory
        df = spark.read.parquet(parquet_dir)
        
        # Verify if 'author_steamid' column exists
        if "author_steamid" not in df.columns:
            raise ValueError("Column 'author_steamid' not found in the dataset.")
        
        # Add a date column for filtering and analysis
        df = df.withColumn("review_date", to_date(col("timestamp_created")))
        
        # Apply date filtering if provided
        if start_date and end_date:
            start_str = start_date.strftime("%Y-%m-%d")
            end_str = end_date.strftime("%Y-%m-%d")
            df = df.filter((col("review_date") >= start_str) & (col("review_date") <= end_str))
        
        # Group by author, count reviews, and get top k
        top_authors_spark = df.groupBy("author_steamid") \
                     .count() \
                     .orderBy(col("count").desc()) \
                     .limit(k)
        
        # Convert to pandas manually using collect()
        top_authors_data = top_authors_spark.collect()
        top_authors = pd.DataFrame([(row["author_steamid"], row["count"]) for row in top_authors_data], 
                                  columns=["author_id", "review_count"])
        
        return top_authors
    except Exception as e:
        raise e

def get_top_authors_by_upvotes(spark, parquet_dir, k=10, start_date=None, end_date=None):
    """
    Find the top k authors with the most total upvotes from a directory of Parquet files.
    
    Parameters:
    - spark: SparkSession object
    - parquet_dir: Path to the directory containing Parquet files
    - k: Number of top authors to return (default: 10)
    - start_date: Optional start date filter (datetime)
    - end_date: Optional end date filter (datetime)
    
    Returns:
    - pandas DataFrame with the top k authors
    """
    try:
        # Load all Parquet files from the directory
        df = spark.read.parquet(parquet_dir)
        
        # Verify if required columns exist
        if "author_steamid" not in df.columns or "votes_up" not in df.columns:
            raise ValueError("Required columns not found in the dataset.")
        
        # Add a date column for filtering and analysis
        df = df.withColumn("review_date", to_date(col("timestamp_created")))
        
        # Apply date filtering if provided
        if start_date and end_date:
            start_str = start_date.strftime("%Y-%m-%d")
            end_str = end_date.strftime("%Y-%m-%d")
            df = df.filter((col("review_date") >= start_str) & (col("review_date") <= end_str))
        
        # Group by author, sum upvotes, and get top k
        top_authors_spark = df.groupBy("author_steamid") \
                     .agg(spark_sum("votes_up").alias("total_upvotes"), count("*").alias("review_count")) \
                     .orderBy(col("total_upvotes").desc()) \
                     .limit(k)
        
        # Convert to pandas manually using collect()
        top_authors_data = top_authors_spark.collect()
        top_authors = pd.DataFrame([
            (row["author_steamid"], row["total_upvotes"], row["review_count"]) 
            for row in top_authors_data
        ], columns=["author_id", "total_upvotes", "review_count"])
        
        return top_authors
    except Exception as e:
        raise e 

def get_all_engagement_metrics(spark, parquet_dir, games_k=10, authors_k=10, start_date=None, end_date=None):
    """
    Calculate all engagement metrics in a single pass through the data:
    - Top k games by review count
    - Top k authors by review count
    - Top k authors by total upvotes
    
    Parameters:
    - spark: SparkSession object
    - parquet_dir: Path to the directory containing Parquet files
    - games_k: Number of top games to return (default: 10)
    - authors_k: Number of top authors to return (default: 10)
    - start_date: Optional start date filter (datetime)
    - end_date: Optional end date filter (datetime)
    
    Returns:
    - Dictionary containing all three dataframes
    """
    try:
        # Load all Parquet files from the directory
        df = spark.read.parquet(parquet_dir)
        
        # Verify if required columns exist
        required_columns = ["game", "author_steamid", "votes_up", "timestamp_created"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Required columns missing: {missing_columns}")
        
        # Add a date column for filtering and analysis
        df = df.withColumn("review_date", to_date(col("timestamp_created")))
        
        # Apply date filtering if provided
        if start_date and end_date:
            start_str = start_date.strftime("%Y-%m-%d")
            end_str = end_date.strftime("%Y-%m-%d")
            df = df.filter((col("review_date") >= start_str) & (col("review_date") <= end_str))
        
        # Cache the filtered dataframe to avoid recomputing for each query
        df.cache()
        
        # 1. Calculate top games by review count
        top_games_spark = df.groupBy("game") \
                     .count() \
                     .orderBy(col("count").desc()) \
                     .limit(games_k)
        
        # 2. Calculate top authors by review count
        top_authors_by_count_spark = df.groupBy("author_steamid") \
                     .count() \
                     .orderBy(col("count").desc()) \
                     .limit(authors_k)
        
        # 3. Calculate top authors by total upvotes
        top_authors_by_upvotes_spark = df.groupBy("author_steamid") \
                     .agg(spark_sum("votes_up").alias("total_upvotes"), count("*").alias("review_count")) \
                     .orderBy(col("total_upvotes").desc()) \
                     .limit(authors_k)
        
        # Convert to pandas DataFrames
        top_games = pd.DataFrame([(row["game"], row["count"]) for row in top_games_spark.collect()], 
                                columns=["game", "count"])
        
        top_authors_by_count = pd.DataFrame([(row["author_steamid"], row["count"]) for row in top_authors_by_count_spark.collect()], 
                                          columns=["author_id", "review_count"])
        
        top_authors_by_upvotes = pd.DataFrame([
            (row["author_steamid"], row["total_upvotes"], row["review_count"]) 
            for row in top_authors_by_upvotes_spark.collect()
        ], columns=["author_id", "total_upvotes", "review_count"])
        
        # Uncache the dataframe
        df.unpersist()
        
        # Return all results as a dictionary
        return {
            "top_games": top_games,
            "top_authors_by_count": top_authors_by_count,
            "top_authors_by_upvotes": top_authors_by_upvotes
        }
    except Exception as e:
        raise e 