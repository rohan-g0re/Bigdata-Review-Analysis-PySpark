import streamlit as st
import plotly.express as px
import pandas as pd
from src.utils.spark_session import get_spark_session
from src.tabs.top_games_tab.analysis import get_top_k_games_by_reviews
from src.utils.stream_data_utils import stream_data_manager
from src.utils.ui_utils import (
    create_auto_refresh_toggle,
    auto_refresh_component,
    display_data_freshness,
    run_with_spinner,
    create_refresh_button
)
from src.streaming.config.settings import DASHBOARD_REFRESH_INTERVAL

def render_batch_mode(k: int):
    """Render batch analysis mode for top games"""
    # Path to parquet files
    parquet_dir = "D:/STUFF/Projects/BigData_Project/data/all_reviews/cleaned_reviews"
    
    # Run analysis when user clicks button
    if st.button("Analyze Top Games"):
        with st.spinner("Analyzing data... This may take a minute..."):
            # Get Spark session
            spark = get_spark_session(app_name="TopGamesAnalysis")
            
            try:
                # Get top k games
                top_games = get_top_k_games_by_reviews(spark, parquet_dir, k)
                
                if top_games is not None:
                    # Store in session state for reuse
                    st.session_state.top_games_batch_data = top_games
                    display_top_games_results(top_games, k)
            except Exception as e:
                st.error(f"Error during analysis: {e}")
            finally:
                # Stop Spark session
                spark.stop()
    
    # Display cached results if available
    if "top_games_batch_data" in st.session_state:
        display_top_games_results(st.session_state.top_games_batch_data, k)

def render_streaming_mode(k: int):
    """Render streaming mode for top games"""
    refresh_container = st.container()
    
    with refresh_container:
        col1, col2 = st.columns([3, 1])
        
        with col1:
            auto_refresh = create_auto_refresh_toggle("top_games_auto_refresh")
            
        with col2:
            manual_refresh = create_refresh_button("top_games_refresh")
    
    # Check if we should refresh the data
    should_refresh = manual_refresh
    
    if auto_refresh:
        should_refresh = should_refresh or auto_refresh_component(
            refresh_interval=DASHBOARD_REFRESH_INTERVAL,
            session_key="top_games_last_refresh"
        )
    
    # Get data freshness info for display
    freshness_info = stream_data_manager.get_data_freshness()
    display_data_freshness(freshness_info, refresh_container)
    
    # Load streaming data
    top_games = stream_data_manager.get_top_games(
        limit=k, 
        force_refresh=should_refresh
    )
    
    if top_games is not None and not top_games.empty:
        # Create a copy to avoid modifying the original
        display_df = top_games.copy()
        
        # Normalize column names for display
        column_mappings = {
            # Possible game id/name columns
            'game_id': 'game',
            'app_id': 'game',
            'game_name': 'game',
            'name': 'game',
            
            # Possible count columns
            'review_count': 'count',
            'count': 'count',
            'game_count': 'count',
            'total': 'count'
        }
        
        # Apply mappings for columns that exist
        for old_col, new_col in column_mappings.items():
            if old_col in display_df.columns:
                display_df = display_df.rename(columns={old_col: new_col})
        
        # Check if we have the minimum required columns after mapping
        if 'game' in display_df.columns and 'count' in display_df.columns:
            display_top_games_results(display_df, k)
        else:
            st.warning(f"Streaming data format missing required columns. Available columns: {list(display_df.columns)}")
            
            # Create default columns if they don't exist
            if 'game' not in display_df.columns and len(display_df.columns) > 0:
                # Use the first column as the game name
                display_df = display_df.rename(columns={display_df.columns[0]: 'game'})
                
            if 'count' not in display_df.columns and len(display_df.columns) > 1:
                # Use the second column as the count
                display_df = display_df.rename(columns={display_df.columns[1]: 'count'})
                
            # Try displaying with what we have
            if 'game' in display_df.columns:
                st.write("Showing data with limited formatting:")
                st.dataframe(display_df)
    else:
        st.info("No streaming data available yet. Start the streaming system to see real-time data.")
        st.caption("Run `python -m src.streaming.run_system --component all --dashboard` to start the system")

def display_top_games_results(top_games: pd.DataFrame, k: int):
    """Display top games analysis results"""
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

def render_top_games_tab(data_mode: str = "batch"):
    """Content for the Top Games tab"""
    st.subheader("Discover the most reviewed games on Steam")
    
    # Input for number of top games
    k = st.slider("Number of Top Games to Display", min_value=5, max_value=50, value=10, step=5)
    
    # Show different content based on mode
    if data_mode == "batch":
        st.info("Batch Analysis Mode: Data will be analyzed directly from Parquet files")
        render_batch_mode(k)
    else:
        st.info("Streaming Mode: Data is from the real-time Kafka pipeline")
        render_streaming_mode(k) 