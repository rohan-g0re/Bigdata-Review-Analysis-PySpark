import os
import time
import pandas as pd
import streamlit as st
import altair as alt
from datetime import datetime, timedelta
import json

from src.streaming.config.settings import (
    STREAM_RESULTS_DIR,
    DASHBOARD_REFRESH_INTERVAL
)
from src.utils.stream_data_utils import stream_data_manager
from src.utils.ui_utils import (
    create_auto_refresh_toggle,
    auto_refresh_component,
    display_data_freshness,
    run_with_spinner,
    create_refresh_button
)
from src.utils.stream_control import is_streaming, start_streaming

def render_streaming_tab():
    """Render the streaming tab UI"""
    st.header("Real-time Streaming Analytics")
    
    # Add info about the streaming system
    with st.expander("About Streaming Analytics", expanded=False):
        st.markdown("""
        This dashboard displays real-time analytics from the Kafka streaming pipeline. 
        The system reads Steam game reviews from Parquet files and streams them through Kafka 
        for real-time processing and analysis.
        
        **Data shown includes:**
        - Top games by review count
        - Sentiment analysis by game
        - Time distribution of reviews
        - Recent reviews stream
        
        The data refreshes automatically every 5 seconds when auto-refresh is enabled.
        """)
    
    # Check if streaming results directory exists and create it if needed
    os.makedirs(STREAM_RESULTS_DIR, exist_ok=True)
    
    # Check if streaming is active
    if not is_streaming():
        st.warning("Streaming is not active. Switch to 'Real-time Streaming' mode in the sidebar to start.")
        start_button = st.button("Start Streaming Now")
        if start_button:
            with st.spinner("Starting Kafka streaming..."):
                success = start_streaming()
                if success:
                    st.success("Streaming started successfully!")
                    # Don't use experimental_rerun as it's causing issues
                else:
                    st.error("Failed to start streaming. See logs for details.")
        return
    
    # Controls for auto-refresh
    refresh_container = st.container()
    
    with refresh_container:
        col1, col2 = st.columns([3, 1])
        
        with col1:
            auto_refresh = create_auto_refresh_toggle("streaming_tab_auto_refresh", default=True)
            
        with col2:
            manual_refresh = create_refresh_button("streaming_tab_refresh")
    
    # Check if we should refresh the data
    refresh_status = st.empty()
    
    # Either manual refresh or auto-refresh can trigger data reload
    should_refresh = manual_refresh
    
    # If auto-refresh is enabled, use our custom auto-refresh component
    if auto_refresh:
        # Use a unique session key for each rendering to prevent interference
        auto_refresh_key = "streaming_refresh"
        # Call the auto-refresh component and check if we should refresh
        should_auto_refresh = auto_refresh_component(
            refresh_interval=DASHBOARD_REFRESH_INTERVAL,
            session_key=auto_refresh_key
        )
        # Combine manual and auto refresh triggers
        should_refresh = should_refresh or should_auto_refresh
    
    # Get data freshness info for display
    freshness_info = stream_data_manager.get_data_freshness()
    display_data_freshness(freshness_info, refresh_container)
    
    # Load streaming data
    top_games_df = stream_data_manager.get_top_games(limit=10, force_refresh=should_refresh)
    sentiment_df = stream_data_manager.get_sentiment_analysis(limit=10, force_refresh=should_refresh)
    time_df = stream_data_manager.get_time_distribution(force_refresh=should_refresh)
    recent_reviews_df = stream_data_manager.get_recent_reviews(limit=5, force_refresh=should_refresh)
    
    # Display visualizations in a grid layout
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Top Games by Review Count")
        if top_games_df is not None and not top_games_df.empty:
            # Make sure we have the necessary columns or create substitutes
            chart_df = top_games_df.copy()
            id_column = None
            count_column = None
            
            for possible_id in ['game_id', 'app_id', 'game', 'id']:
                if possible_id in chart_df.columns:
                    id_column = possible_id
                    break
            
            for possible_count in ['review_count', 'count', 'total']:
                if possible_count in chart_df.columns:
                    count_column = possible_count
                    break
            
            if id_column is None and len(chart_df.columns) > 0:
                id_column = chart_df.columns[0]
            
            if count_column is None and len(chart_df.columns) > 1:
                count_column = chart_df.columns[1]
            
            if id_column and count_column:
                # Create chart
                try:
                    chart = alt.Chart(chart_df).mark_bar().encode(
                        x=alt.X(f'{count_column}:Q', title='Number of Reviews'),
                        y=alt.Y(f'{id_column}:N', title='Game ID', sort='-x'),
                        tooltip=[id_column, count_column]
                    ).properties(height=300)
                    
                    st.altair_chart(chart, use_container_width=True)
                except Exception as e:
                    st.error(f"Error creating chart: {str(e)}")
                    st.dataframe(chart_df)
            else:
                st.info("Column names in the data don't match expectations. Showing raw data:")
                st.dataframe(chart_df)
        else:
            st.info("No top games data available yet. Data will appear as reviews are processed.")
    
    with col2:
        st.subheader("Sentiment Analysis by Game")
        if sentiment_df is not None and not sentiment_df.empty:
            # Make sure we have the necessary columns or create substitutes
            chart_df = sentiment_df.copy()
            id_column = None
            sentiment_column = None
            
            for possible_id in ['game_id', 'app_id', 'game', 'id']:
                if possible_id in chart_df.columns:
                    id_column = possible_id
                    break
            
            for possible_sentiment in ['avg_sentiment', 'sentiment', 'score']:
                if possible_sentiment in chart_df.columns:
                    sentiment_column = possible_sentiment
                    break
            
            if id_column is None and len(chart_df.columns) > 0:
                id_column = chart_df.columns[0]
            
            if sentiment_column is None and len(chart_df.columns) > 1:
                sentiment_column = chart_df.columns[1]
            
            if id_column and sentiment_column:
                # Create chart
                try:
                    chart = alt.Chart(chart_df).mark_bar().encode(
                        x=alt.X(f'{sentiment_column}:Q', title='Average Sentiment (0-1)'),
                        y=alt.Y(f'{id_column}:N', title='Game ID', sort='-x'),
                        color=alt.Color(f'{sentiment_column}:Q', scale=alt.Scale(scheme='blueorange'), title='Sentiment'),
                        tooltip=[id_column, sentiment_column] + ([count_column] if 'count_column' in locals() and count_column in chart_df.columns else [])
                    ).properties(height=300)
                    
                    st.altair_chart(chart, use_container_width=True)
                except Exception as e:
                    st.error(f"Error creating chart: {str(e)}")
                    st.dataframe(chart_df)
            else:
                st.info("Column names in the data don't match expectations. Showing raw data:")
                st.dataframe(chart_df)
        else:
            st.info("No sentiment analysis data available yet. Data will appear as reviews are processed.")
    
    st.subheader("Review Time Distribution")
    if time_df is not None and not time_df.empty:
        # Make sure we have the necessary columns or create substitutes
        chart_df = time_df.copy()
        time_column = None
        count_column = None
        
        for possible_time in ['hour', 'time', 'date', 'timestamp']:
            if possible_time in chart_df.columns:
                time_column = possible_time
                break
        
        for possible_count in ['count', 'review_count', 'total']:
            if possible_count in chart_df.columns:
                count_column = possible_count
                break
        
        if time_column is None and len(chart_df.columns) > 0:
            time_column = chart_df.columns[0]
        
        if count_column is None and len(chart_df.columns) > 1:
            count_column = chart_df.columns[1]
        
        if time_column and count_column:
            # Try to convert time column to datetime if it's not already
            if pd.api.types.is_datetime64_any_dtype(chart_df[time_column]) == False:
                try:
                    chart_df[time_column] = pd.to_datetime(chart_df[time_column])
                except:
                    pass
            
            # Create chart
            try:
                chart = alt.Chart(chart_df).mark_line().encode(
                    x=alt.X(f'{time_column}:T', title='Hour'),
                    y=alt.Y(f'{count_column}:Q', title='Number of Reviews'),
                    tooltip=[time_column, count_column]
                ).properties(height=250)
                
                st.altair_chart(chart, use_container_width=True)
            except Exception as e:
                st.error(f"Error creating chart: {str(e)}")
                st.dataframe(chart_df)
        else:
            st.info("Column names in the data don't match expectations. Showing raw data:")
            st.dataframe(chart_df)
    else:
        st.info("No time distribution data available yet. Data will appear as reviews are processed.")
    
    st.subheader("Recent Reviews")
    if recent_reviews_df is not None and not recent_reviews_df.empty:
        # Find the important columns or use defaults
        app_id_col = next((col for col in ['app_id', 'game_id', 'game'] if col in recent_reviews_df.columns), None)
        review_col = next((col for col in ['review', 'review_text', 'text'] if col in recent_reviews_df.columns), None)
        recommend_col = next((col for col in ['recommended', 'voted_up', 'positive'] if col in recent_reviews_df.columns), None)
        timestamp_col = next((col for col in ['timestamp', 'timestamp_created', 'date', 'time'] if col in recent_reviews_df.columns), None)
        
        # Add some styling to the reviews
        for i, review in enumerate(recent_reviews_df.head(5).itertuples()):
            review_container = st.container()
            with review_container:
                col1, col2 = st.columns([1, 5])
                with col1:
                    # Show positive/negative if the column exists
                    if recommend_col and hasattr(review, recommend_col) and getattr(review, recommend_col):
                        st.markdown("👍 Positive")
                    else:
                        st.markdown("👎 Negative")
                    
                    # Show game ID if available
                    if app_id_col and hasattr(review, app_id_col):
                        st.caption(f"Game: {getattr(review, app_id_col)}")
                
                with col2:
                    # Show review text if available
                    if review_col and hasattr(review, review_col):
                        st.markdown(f"**{getattr(review, review_col)}**")
                    
                    # Show timestamp if available
                    if timestamp_col and hasattr(review, timestamp_col):
                        st.caption(f"Posted: {getattr(review, timestamp_col)}")
                
                st.divider()
    else:
        st.info("No recent reviews available yet. Reviews will appear as they are processed.")
    
    # Stream statistics
    if freshness_info["has_data"]:
        st.subheader("Streaming System Statistics")
        stats_col1, stats_col2, stats_col3 = st.columns(3)
        
        # Use variables from above to calculate metrics
        with stats_col1:
            if top_games_df is not None and not top_games_df.empty and 'count_column' in locals() and count_column in top_games_df.columns:
                review_count = top_games_df[count_column].sum()
            else:
                review_count = 0
            st.metric("Total Reviews Processed", f"{review_count:,}")
        
        with stats_col2:
            game_count = len(top_games_df) if top_games_df is not None and not top_games_df.empty else 0
            st.metric("Unique Games", f"{game_count:,}")
        
        with stats_col3:
            if sentiment_df is not None and not sentiment_df.empty and 'sentiment_column' in locals() and sentiment_column in sentiment_df.columns:
                avg_sentiment = sentiment_df[sentiment_column].mean()
            else:
                avg_sentiment = 0
            st.metric("Average Sentiment", f"{avg_sentiment:.2f}") 