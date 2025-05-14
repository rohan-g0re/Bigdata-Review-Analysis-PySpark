import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import plotly.express as px

def get_top_10_games_by_reviews(parquet_dir):
    """
    Find the top 10 games with the most reviews from a directory of Parquet files,
    grouping by the 'game' column, and display results with a bar chart.
    
    Parameters:
    - parquet_dir: Path to the directory containing Parquet files
    """
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("TopGamesByReviews") \
        .config("spark.driver.memory", "8g") \
        .getOrCreate()
    
    try:
        # Load all Parquet files from the directory
        df = spark.read.parquet(parquet_dir)
        
        # Verify if 'game' column exists
        if "game" not in df.columns:
            spark.stop()
            raise ValueError("Column 'game' not found in the dataset. Available columns: " + ", ".join(df.columns))
        
        # Group by 'game', count reviews, and get top 10
        top_games_spark = df.groupBy("game") \
                     .count() \
                     .orderBy(col("count").desc()) \
                     .limit(10)
        
        # Convert to pandas manually using collect() instead of toPandas()
        top_games_data = top_games_spark.collect()
        top_games = pd.DataFrame([(row["game"], row["count"]) for row in top_games_data], 
                                 columns=["game", "count"])
        
        # Display results as a table
        print("Top 10 Games by Review Count:")
        print(top_games)
        
        # Create bar chart
        fig = px.bar(
            top_games,
            x="game",
            y="count",
            title="Top 10 Games by Review Count",
            labels={"game": "Game Name", "count": "Number of Reviews"},
            color="game",
            text="count"
        )
        fig.update_layout(xaxis_tickangle=45, showlegend=False)
        fig.show()
        
        return top_games
        
    except Exception as e:
        print(f"Error: {e}")
        return None
    finally:
        # Stop Spark session
        spark.stop()

if __name__ == "__main__":
    # Execute the query
    parquet_dir = "D:/STUFF/Projects/BigData_Project/data/all_reviews/cleaned_reviews"
    get_top_10_games_by_reviews(parquet_dir) 