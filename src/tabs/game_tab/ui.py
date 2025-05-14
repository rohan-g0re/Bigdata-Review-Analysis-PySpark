import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from src.utils.spark_session import get_spark_session
from src.tabs.game_tab.analysis import get_game_info
from src.tabs.game_tab.visualization import create_time_series_plot, display_review_cards

def render_game_info_tab():
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
            # Get Spark session
            spark = get_spark_session(app_name="GameInfoAnalysis")
            
            try:
                # Get game info with date filtering
                with st.expander("Debug Information", expanded=show_debug):
                    game_info = get_game_info(spark, parquet_dir, game_name, start_datetime, end_datetime)
                
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
                        
                        # Create and display time series plot
                        fig = create_time_series_plot(game_info)
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
                        display_review_cards(game_info['top_upvoted_reviews'], vote_type="upvotes")
                    else:
                        st.info("No upvoted reviews data available.")
                    
                    # Display top 10 reviews with most funny votes
                    st.subheader("Top Reviews by Funny Votes")
                    if 'top_funny_reviews' in game_info and not game_info['top_funny_reviews'].empty:
                        display_review_cards(game_info['top_funny_reviews'], vote_type="funny")
                    else:
                        st.info("No funny-voted reviews data available.")
                    
                    st.markdown("---")
                else:
                    st.error(f"Game '{game_name}' not found in the dataset within the selected date range. Please check the spelling, try another game, or adjust the date range.")
            finally:
                # Stop Spark session
                spark.stop() 