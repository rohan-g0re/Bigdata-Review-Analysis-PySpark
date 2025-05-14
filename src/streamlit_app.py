import streamlit as st
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, round as spark_round, to_date, count, year, month, date_format
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

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

def get_game_info(parquet_dir, game_name, start_date=None, end_date=None):
    """
    Get specific information about a game from the Parquet files.
    
    Parameters:
    - parquet_dir: Path to the directory containing Parquet files
    - game_name: Name of the game to search for
    - start_date: Optional start date filter (datetime)
    - end_date: Optional end date filter (datetime)
    
    Returns:
    - Dictionary with game information
    """
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("GameInfo") \
        .config("spark.driver.memory", "8g") \
        .getOrCreate()
    
    try:
        # Load all Parquet files from the directory
        df = spark.read.parquet(parquet_dir)
        
        # Print schema to debug timestamp format
        st.write("Debugging timestamp format:")
        df.printSchema()
        
        # Filter for the specific game
        game_df = df.filter(col("game") == game_name)
        
        # Check if we found any reviews for this game
        initial_count = game_df.count()
        st.write(f"Initial count for {game_name}: {initial_count}")
        
        # Add a date column for filtering and analysis
        game_df = game_df.withColumn("review_date", to_date(col("timestamp_created")))
        
        # Get time series data for plotting (before date filtering)
        all_time_series_data = None
        
        # Apply date filtering if provided
        if start_date and end_date:
            # Show the first few rows to check timestamp_created format
            sample_data = game_df.select("timestamp_created").limit(5).collect()
            st.write("Sample timestamp data:", [str(row["timestamp_created"]) for row in sample_data])
            
            # Try different approach based on the actual data type
            try:
                # Convert Python datetime to string format YYYY-MM-DD for Spark SQL
                start_str = start_date.strftime("%Y-%m-%d")
                end_str = end_date.strftime("%Y-%m-%d")
                
                st.write(f"Filtering between {start_str} and {end_str}")
                
                # Filter by date range
                game_df = game_df.filter((col("review_date") >= start_str) & (col("review_date") <= end_str))
                
                # Check filtered count
                filtered_count = game_df.count()
                st.write(f"After date filtering: {filtered_count} reviews")
            except Exception as e:
                st.error(f"Error during date filtering: {e}")
                # Continue without date filtering
        
        # Check if any records were found
        review_count = game_df.count()
        if review_count == 0:
            return None
        
        # Calculate average playtime at review, handling null values
        avg_playtime = game_df.select(
            spark_round(avg("author_playtime_at_review"), 2)
        ).collect()[0][0]
        
        # Get time series data for reviews by day (instead of month)
        # Use the review_date directly as it's already a date
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
                "review_text": row["review"][:300] + "..." if len(row["review"]) > 300 else row["review"],
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
                "review_text": row["review"][:300] + "..." if len(row["review"]) > 300 else row["review"],
                "votes_funny": row["votes_funny"],
                "date": row["timestamp_created"].strftime("%Y-%m-%d"),
                "playtime_hrs": round(row["author_playtime_at_review"] / 60, 1) if row["author_playtime_at_review"] else 0
            } for row in top_funny_reviews
        ])
        
        # Print debug info
        st.write(f"Final metrics - Reviews: {review_count}, Avg Playtime: {avg_playtime}")
        st.write(f"Time series data points: {len(time_series_data)}")
        
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
        st.error(f"Error: {e}")
        return None
    finally:
        # Stop Spark session
        spark.stop()

def top_games_tab():
    """Content for the Top Games tab"""
    st.subheader("Discover the most reviewed games on Steam")
    
    # Input for number of top games
    k = st.slider("Number of Top Games to Display", min_value=5, max_value=50, value=10, step=5)
    
    # Path to parquet files
    parquet_dir = "D:/STUFF/Projects/BigData_Project/data/all_reviews/cleaned_reviews"
    
    # Run analysis when user clicks button
    if st.button("Analyze Top Games"):
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

def game_info_tab():
    """Content for the Game Info tab"""
    st.subheader("Search for a specific game")
    
    # Path to parquet files
    parquet_dir = "D:/STUFF/Projects/BigData_Project/data/all_reviews/cleaned_reviews"
    
    # Game search input
    game_name = st.text_input("Enter a game name:", placeholder="e.g. Counter-Strike 2")
    
    # Date range filter
    st.subheader("Filter by Date Range")
    
    col1, col2 = st.columns(2)
    
    # Default date range (last 5 years)
    default_end_date = datetime.now()
    default_start_date = default_end_date - timedelta(days=365*5)
    
    with col1:
        start_date = st.date_input("Start Date", value=default_start_date)
    
    with col2:
        end_date = st.date_input("End Date", value=default_end_date)
    
    # Convert date inputs to datetime objects
    start_datetime = datetime.combine(start_date, datetime.min.time())
    end_datetime = datetime.combine(end_date, datetime.max.time())
    
    # Add note about date filter
    st.caption("Note: Date range filters reviews by their creation date")
    
    # Add a debug toggle
    show_debug = st.checkbox("Show debug information")
    
    if st.button("Search Game") and game_name:
        with st.spinner("Searching for game information..."):
            # Get game info with date filtering
            with st.expander("Debug Information", expanded=show_debug):
                game_info = get_game_info(parquet_dir, game_name, start_datetime, end_datetime)
            
            if game_info:
                # Create a nice card-like display
                st.markdown("---")
                st.markdown(f"## {game_info['game_name']}")
                st.caption(f"Data filtered from {start_date} to {end_date}")
                
                # Use columns for the metrics
                col1, col2 = st.columns(2)
                
                with col1:
                    st.metric("Total Reviews", f"{game_info['total_reviews']:,}")
                
                with col2:
                    # Convert to hours if available
                    if game_info['avg_playtime'] is not None:
                        avg_hours = game_info['avg_playtime'] / 60  # Convert minutes to hours
                        st.metric("Average Playtime at Review", f"{avg_hours:.1f} hours")
                    else:
                        st.metric("Average Playtime at Review", "Not available")
                
                # Add time series plot if data is available
                if 'time_series_data' in game_info and not game_info['time_series_data'].empty:
                    st.subheader("Reviews Over Time")
                    
                    # Create time series plot
                    fig = px.line(
                        game_info['time_series_data'], 
                        x='date', 
                        y='count',
                        title=f"Daily Review Count for {game_info['game_name']}",
                        labels={"date": "Date", "count": "Number of Reviews"},
                        markers=True
                    )
                    
                    # Add area under the line for better visualization
                    fig.add_trace(
                        go.Scatter(
                            x=game_info['time_series_data']['date'],
                            y=game_info['time_series_data']['count'],
                            fill='tozeroy',
                            fillcolor='rgba(0, 176, 246, 0.2)',
                            line=dict(color='rgba(0, 176, 246, 0.7)'),
                            name="Daily Reviews"
                        )
                    )
                    
                    # Update layout for better readability
                    fig.update_layout(
                        xaxis_title="Date",
                        yaxis_title="Number of Reviews",
                        hovermode="x unified",
                        legend_title="Legend"
                    )
                    
                    # Show the plot
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Add a data table with the time series data
                    with st.expander("View Daily Review Data"):
                        # Sort from newest to oldest
                        sorted_data = game_info['time_series_data'].sort_values(by='date', ascending=False)
                        st.dataframe(sorted_data, use_container_width=True)
                else:
                    st.warning("Not enough time-based data available to create a timeline chart.")
                
                # Display top 10 reviews with most upvotes
                st.subheader("Top Reviews by Upvotes")
                if 'top_upvoted_reviews' in game_info and not game_info['top_upvoted_reviews'].empty:
                    for i, row in game_info['top_upvoted_reviews'].iterrows():
                        with st.container():
                            st.markdown(f"""
                            <div style="border: 1px solid #ddd; padding: 10px; border-radius: 5px; margin-bottom: 10px;">
                                <div style="display: flex; justify-content: space-between; margin-bottom: 5px;">
                                    <span style="color: #888;">User: {row['author_id'][:8]}...</span>
                                    <span style="color: #888;">Date: {row['date']}</span>
                                </div>
                                <div style="margin-bottom: 10px;">{row['review_text']}</div>
                                <div style="display: flex; justify-content: space-between;">
                                    <span>👍 <strong>{row['votes_up']}</strong> upvotes</span>
                                    <span>⏱️ <strong>{row['playtime_hrs']}</strong> hours played</span>
                                </div>
                            </div>
                            """, unsafe_allow_html=True)
                else:
                    st.info("No upvoted reviews data available.")
                
                # Display top 10 reviews with most funny votes
                st.subheader("Top Reviews by Funny Votes")
                if 'top_funny_reviews' in game_info and not game_info['top_funny_reviews'].empty:
                    for i, row in game_info['top_funny_reviews'].iterrows():
                        with st.container():
                            st.markdown(f"""
                            <div style="border: 1px solid #ddd; padding: 10px; border-radius: 5px; margin-bottom: 10px;">
                                <div style="display: flex; justify-content: space-between; margin-bottom: 5px;">
                                    <span style="color: #888;">User: {row['author_id'][:8]}...</span>
                                    <span style="color: #888;">Date: {row['date']}</span>
                                </div>
                                <div style="margin-bottom: 10px;">{row['review_text']}</div>
                                <div style="display: flex; justify-content: space-between;">
                                    <span>😂 <strong>{row['votes_funny']}</strong> funny votes</span>
                                    <span>⏱️ <strong>{row['playtime_hrs']}</strong> hours played</span>
                                </div>
                            </div>
                            """, unsafe_allow_html=True)
                else:
                    st.info("No funny-voted reviews data available.")
                
                st.markdown("---")
            else:
                st.error(f"Game '{game_name}' not found in the dataset within the selected date range. Please check the spelling, try another game, or adjust the date range.")

def main():
    st.set_page_config(page_title="Steam Reviews Analysis", layout="wide")
    
    st.title("Steam Game Reviews Analysis")
    
    # Create tabs
    tab1, tab2 = st.tabs(["Top Games", "Game Info"])
    
    with tab1:
        top_games_tab()
    
    with tab2:
        game_info_tab()

if __name__ == "__main__":
    main() 