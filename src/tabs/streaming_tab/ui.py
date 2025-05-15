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
        
        The data refreshes automatically every few seconds.
        """)
    
    # Check if streaming results directory exists
    if not os.path.exists(STREAM_RESULTS_DIR):
        st.warning(f"Streaming results directory not found: {STREAM_RESULTS_DIR}")
        st.info("Start the streaming consumer to see real-time analytics")
        st.caption("Run `python -m src.streaming.run_system --component all --dashboard` to start the system")
        return
    
    # Controls for auto-refresh
    refresh_container = st.container()
    
    with refresh_container:
        col1, col2 = st.columns([3, 1])
        
        with col1:
            auto_refresh = create_auto_refresh_toggle("streaming_tab_auto_refresh")
            
        with col2:
            manual_refresh = create_refresh_button("streaming_tab_refresh")
    
    # Check if we should refresh the data
    should_refresh = manual_refresh
    
    if auto_refresh:
        should_refresh = should_refresh or auto_refresh_component(
            refresh_interval=DASHBOARD_REFRESH_INTERVAL,
            session_key="streaming_tab_last_refresh"
        )
    
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
            # Create chart
            chart = alt.Chart(top_games_df).mark_bar().encode(
                x=alt.X('review_count:Q', title='Number of Reviews'),
                y=alt.Y('game_id:N', title='Game ID', sort='-x'),
                tooltip=['game_id', 'review_count']
            ).properties(height=300)
            
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("No top games data available yet")
    
    with col2:
        st.subheader("Sentiment Analysis by Game")
        if sentiment_df is not None and not sentiment_df.empty:
            # Create chart
            chart = alt.Chart(sentiment_df).mark_bar().encode(
                x=alt.X('avg_sentiment:Q', title='Average Sentiment (0-1)'),
                y=alt.Y('game_id:N', title='Game ID', sort='-x'),
                color=alt.Color('avg_sentiment:Q', scale=alt.Scale(scheme='blueorange'), title='Sentiment'),
                tooltip=['game_id', 'review_count', 'avg_sentiment']
            ).properties(height=300)
            
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("No sentiment analysis data available yet")
    
    st.subheader("Review Time Distribution")
    if time_df is not None and not time_df.empty:
        # Create chart
        chart = alt.Chart(time_df).mark_line().encode(
            x=alt.X('hour:T', title='Hour'),
            y=alt.Y('count:Q', title='Number of Reviews'),
            tooltip=['hour', 'count']
        ).properties(height=250)
        
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No time distribution data available yet")
    
    st.subheader("Recent Reviews")
    if recent_reviews_df is not None and not recent_reviews_df.empty:
        # Add some styling to the reviews
        for i, review in enumerate(recent_reviews_df.sort_values('timestamp', ascending=False).head(5).itertuples()):
            review_container = st.container()
            with review_container:
                col1, col2 = st.columns([1, 5])
                with col1:
                    if hasattr(review, 'recommended') and review.recommended:
                        st.markdown("👍 Positive")
                    else:
                        st.markdown("👎 Negative")
                    st.caption(f"Game: {review.app_id}")
                with col2:
                    if hasattr(review, 'review_text'):
                        st.markdown(f"**{review.review_text}**")
                    if hasattr(review, 'timestamp'):
                        st.caption(f"Posted: {review.timestamp}")
                st.divider()
    else:
        st.info("No recent reviews available yet")
    
    # Stream statistics
    if freshness_info["has_data"]:
        st.subheader("Streaming System Statistics")
        stats_col1, stats_col2, stats_col3 = st.columns(3)
        
        # These would be better if we had the actual metrics from Kafka
        with stats_col1:
            review_count = sum(top_games_df['review_count']) if top_games_df is not None and not top_games_df.empty else 0
            st.metric("Total Reviews Processed", f"{review_count:,}")
        
        with stats_col2:
            game_count = len(top_games_df) if top_games_df is not None and not top_games_df.empty else 0
            st.metric("Unique Games", f"{game_count:,}")
        
        with stats_col3:
            avg_sentiment = sentiment_df['avg_sentiment'].mean() if sentiment_df is not None and not sentiment_df.empty else 0
            st.metric("Average Sentiment", f"{avg_sentiment:.2f}") 