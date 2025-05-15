import pandas as pd
from pyspark.sql.functions import col
import os

def get_top_k_games_by_reviews(spark, parquet_dir, k=10):
    """
    Find the top k games with the most reviews from a directory of Parquet files,
    grouping by the 'game' column, and display results with a bar chart.
    
    Parameters:
    - spark: SparkSession object
    - parquet_dir: Path to the directory containing Parquet files
    - k: Number of top games to return (default: 10)
    
    Returns:
    - pandas DataFrame with the top k games
    """
    # Input validation
    if not isinstance(k, int) or k <= 0:
        raise ValueError("k must be a positive integer")
    
    if not os.path.exists(parquet_dir):
        raise FileNotFoundError(f"Parquet directory not found: {parquet_dir}")
    
    try:
        # Load all Parquet files from the directory
        df = spark.read.parquet(parquet_dir)
        
        # Check if DataFrame is empty
        if df.count() == 0:
            raise ValueError("No data found in the parquet files")
        
        # Verify if 'game' column exists
        if "game" not in df.columns:
            raise ValueError("Column 'game' not found in the dataset. Available columns: " + ", ".join(df.columns))
        
        # Group by 'game', count reviews, and get top k
        top_games_spark = df.groupBy("game") \
                     .count() \
                     .orderBy(col("count").desc()) \
                     .limit(k)
        
        # Convert to pandas manually using collect() instead of toPandas()
        top_games_data = top_games_spark.collect()
        
        if not top_games_data:
            raise ValueError("No games found in the dataset")
            
        top_games = pd.DataFrame([(row["game"], row["count"]) for row in top_games_data], 
                                 columns=["game", "count"])
        
        return top_games
        
    except Exception as e:
        # Log the error and re-raise with more context
        error_msg = f"Error processing top games: {str(e)}"
        raise Exception(error_msg) from e 