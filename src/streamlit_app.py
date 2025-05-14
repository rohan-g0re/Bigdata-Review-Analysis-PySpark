import streamlit as st
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import plotly.express as px

def get_top_k_games_by_reviews(parquet_dir, k=10):
    """
    Find the top k games with the most reviews from a directory of Parquet files,
    grouping by the 'game' column, and display results with a bar chart.
    
    Parameters:
    - parquet_dir: Path to the directory containing Parquet files
    - k: Number of top games to return (default: 10)
    
    Returns:
    - pandas DataFrame with the top k games
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
        st.error(f"Error: {e}")
        return None
    finally:
        # Stop Spark session
        spark.stop()

def main():
    st.set_page_config(page_title="Steam Reviews Analysis", layout="wide")
    
    st.title("Steam Game Reviews Analysis")
    st.subheader("Discover the most reviewed games on Steam")
    
    # Sidebar for inputs
    st.sidebar.header("Settings")
    
    # Input for number of top games
    k = st.sidebar.slider("Number of Top Games to Display", min_value=5, max_value=50, value=10, step=5)
    
    # Path to parquet files
    parquet_dir = "D:/STUFF/Projects/BigData_Project/data/all_reviews/cleaned_reviews"
    
    # Run analysis when user clicks button
    if st.sidebar.button("Analyze Top Games"):
        with st.spinner("Analyzing data... This may take a minute..."):
            # Get top k games
            top_games = get_top_k_games_by_reviews(parquet_dir, k)
            
            if top_games is not None:
                # Display results
                st.subheader(f"Top {k} Games by Review Count")
                
                # Display as a table
                st.dataframe(top_games, use_container_width=True)
                
                # Create bar chart
                fig = px.bar(
                    top_games,
                    x="game",
                    y="count",
                    title=f"Top {k} Games by Review Count",
                    labels={"game": "Game Name", "count": "Number of Reviews"},
                    color="game",
                    text="count"
                )
                fig.update_layout(xaxis_tickangle=45)
                st.plotly_chart(fig, use_container_width=True)
                
                # Show statistics
                st.subheader("Statistics")
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Most Reviewed Game", top_games.iloc[0]["game"])
                with col2:
                    st.metric("Most Reviews Count", f"{top_games.iloc[0]['count']:,}")
                with col3:
                    avg_reviews = int(top_games["count"].mean())
                    st.metric("Average Reviews (Top Games)", f"{avg_reviews:,}")

if __name__ == "__main__":
    main() 