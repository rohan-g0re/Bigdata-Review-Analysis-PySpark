import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime, timedelta
from src.utils.spark_session import get_spark_session
from src.tabs.game_tab.analysis import get_game_info
from src.tabs.game_tab.visualization import (
    create_time_series_plot, 
    create_acquisition_pie_chart, 
    display_review_cards,
    create_sentiment_gauge,
    display_sentiment_reviews,
    create_sentiment_distribution_chart
)
from src.utils.constants import LANGUAGES, LANGUAGE_DISPLAY_NAMES
from src.utils.stream_data_utils import stream_data_manager
from src.utils.ui_utils import (
    create_auto_refresh_toggle,
    auto_refresh_component,
    display_data_freshness,
    run_with_spinner,
    create_refresh_button
)
from src.streaming.config.settings import DASHBOARD_REFRESH_INTERVAL

def filter_reviews_by_language(reviews_df, language):
    """Filter reviews DataFrame by language.
    
    Args:
        reviews_df: DataFrame containing reviews
        language: Language code to filter by, or "all" for all languages
        
    Returns:
        Filtered DataFrame
    """
    if language == "all" or reviews_df.empty:
        return reviews_df
    
    return reviews_df[reviews_df['language'] == language].head(10)

def render_batch_mode(parquet_dir, game_name, start_datetime, end_datetime, show_debug):
    """Render the batch mode content for game info tab"""
    # Store game data in session state to avoid reloading
    if 'game_data' not in st.session_state:
        st.session_state.game_data = None
    
    if st.button("Search Game", key="batch_search") and game_name:
        with st.spinner("Searching for game information..."):
            # Get Spark session
            spark = get_spark_session(app_name="GameInfoAnalysis")
            
            try:
                # Get game info with date filtering
                with st.expander("Debug Information", expanded=show_debug):
                    game_info = get_game_info(spark, parquet_dir, game_name, start_datetime, end_datetime)
                
                # Store in session state
                st.session_state.game_data = game_info
            finally:
                # Stop Spark session
                spark.stop()
    
    # Display game data if available in session state
    if st.session_state.game_data:
        display_game_info(
            st.session_state.game_data, 
            start_date=start_datetime.date(), 
            end_date=end_datetime.date()
        )

def render_streaming_mode(game_name):
    """Render the streaming mode content for game info tab"""
    refresh_container = st.container()
    
    with refresh_container:
        col1, col2 = st.columns([3, 1])
        
        with col1:
            auto_refresh = create_auto_refresh_toggle("game_info_auto_refresh")
            
        with col2:
            manual_refresh = create_refresh_button("game_info_refresh")
    
    # Check if we should refresh the data
    should_refresh = manual_refresh
    
    if auto_refresh:
        should_refresh = should_refresh or auto_refresh_component(
            refresh_interval=DASHBOARD_REFRESH_INTERVAL,
            session_key="game_info_last_refresh"
        )
    
    # Get data freshness info for display
    freshness_info = stream_data_manager.get_data_freshness()
    display_data_freshness(freshness_info, refresh_container)
    
    # Search button for streaming data
    if st.button("Search Game", key="stream_search") and game_name:
        # This will force a refresh when searching
        should_refresh = True
    
    # Get sentiment analysis data to see if we have data for this game
    sentiment_df = stream_data_manager.get_sentiment_analysis(
        limit=100, 
        force_refresh=should_refresh
    )
    
    # Get recent reviews to show examples
    recent_reviews_df = stream_data_manager.get_recent_reviews(
        limit=50,
        force_refresh=should_refresh
    )
    
    if sentiment_df is not None and not sentiment_df.empty and game_name:
        # Look for the game in the sentiment data (case insensitive)
        game_id_lower = game_name.lower()
        game_matches = sentiment_df[sentiment_df['game_id'].str.lower().str.contains(game_id_lower)]
        
        if not game_matches.empty:
            # We found matching game(s)
            # Construct a game info dict similar to what the Spark analysis would return
            game_info = {
                'game_name': game_matches.iloc[0]['game_id'],
                'total_reviews': game_matches.iloc[0]['review_count'],
                'sentiment_score': game_matches.iloc[0]['avg_sentiment'],
                'sentiment_analyzed_count': game_matches.iloc[0]['review_count']
            }
            
            # Add reviews if available
            if recent_reviews_df is not None and not recent_reviews_df.empty:
                game_reviews = recent_reviews_df[
                    recent_reviews_df['app_id'].str.lower().str.contains(game_id_lower)
                ]
                
                if not game_reviews.empty:
                    # Split reviews into positive and negative
                    pos_reviews = game_reviews[game_reviews['recommended'] == True]
                    neg_reviews = game_reviews[game_reviews['recommended'] == False]
                    
                    # Add to game info
                    game_info['top_positive_reviews'] = pos_reviews
                    game_info['top_negative_reviews'] = neg_reviews
            
            # Display the game info
            display_game_info(game_info, streaming=True)
        else:
            st.warning(f"Game '{game_name}' not found in streaming data. Try a different game or start the streaming system with more data.")
    elif game_name:
        st.info("No streaming data available or game not found in data. Start the streaming system to see real-time data.")
        st.caption("Run `python -m src.streaming.run_system --component all --dashboard` to start the system")

def display_game_info(game_info, start_date=None, end_date=None, streaming=False):
    """Display game information"""
    # Create a nice card-like display
    st.markdown("---")
    st.markdown(f"## {game_info['game_name']}")
    
    if not streaming and start_date and end_date:
        st.caption(f"Data filtered from {start_date} to {end_date}")
    elif streaming:
        st.caption("Data from real-time streaming pipeline")
    
    # Use columns for the key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Reviews", f"{game_info['total_reviews']:,}")
    
    with col2:
        # Convert to hours if available
        if 'avg_playtime' in game_info and game_info['avg_playtime'] is not None:
            avg_hours = game_info['avg_playtime'] / 60  # Convert minutes to hours
            st.metric("Average Playtime at Review", f"{avg_hours:.1f} hours")
        else:
            st.metric("Average Playtime at Review", "Not available")
    
    with col3:
        # Display early access review count and percentage if available
        if 'early_access_count' in game_info:
            early_access_count = game_info['early_access_count']
            early_access_percent = (early_access_count / game_info['total_reviews']) * 100
            st.metric(
                "Early Access Reviews", 
                f"{early_access_count:,} ({early_access_percent:.1f}%)"
            )
        else:
            st.metric("Early Access Reviews", "Not available")
            
    with col4:
        # Display sentiment score with appropriate label
        sentiment_score = game_info.get('sentiment_score', 0)
        if sentiment_score >= 0.6:
            sentiment_label = "Very Positive"
        elif sentiment_score >= 0.2:
            sentiment_label = "Positive"
        elif sentiment_score >= -0.2:
            sentiment_label = "Mixed"
        elif sentiment_score >= -0.6:
            sentiment_label = "Negative"
        else:
            sentiment_label = "Very Negative"
            
        st.metric(
            "Sentiment Rating", 
            f"{sentiment_label}",
            f"Score: {sentiment_score:.2f}"
        )
        
    # Add sentiment analysis section
    if 'sentiment_score' in game_info:
        st.subheader("Review Sentiment Analysis")
        st.caption(f"Based on {game_info.get('sentiment_analyzed_count', 0):,} analyzed reviews")
        
        # Create two columns for sentiment charts
        sentiment_col1, sentiment_col2 = st.columns(2)
        
        with sentiment_col1:
            # Create and display sentiment gauge chart
            gauge_fig = create_sentiment_gauge(game_info['sentiment_score'])
            st.plotly_chart(gauge_fig, use_container_width=True)
            
            # Information about what the score means
            with st.expander("What does this score mean?"):
                st.markdown("""
                The sentiment score ranges from -1 (very negative) to +1 (very positive):
                - **> 0.6**: Very Positive - Players strongly enjoy the game
                - **0.2 to 0.6**: Positive - Players generally like the game
                - **-0.2 to 0.2**: Mixed - Players have varied opinions
                - **-0.6 to -0.2**: Negative - Players generally dislike the game
                - **< -0.6**: Very Negative - Players strongly dislike the game
                
                This analysis is performed using an AI language model that evaluates the emotional tone of each review.
                """)
    
    # Add time series plot and acquisition method pie chart if available
    if 'time_series_data' in game_info and not game_info['time_series_data'].empty:
        st.subheader("Reviews Over Time and Acquisition Method")
        
        # Create two columns for the charts
        chart_col1, chart_col2 = st.columns(2)
        
        with chart_col1:
            # Create and display time series plot
            time_fig = create_time_series_plot(game_info)
            st.plotly_chart(time_fig, use_container_width=True)
        
        with chart_col2:
            # Create and display pie chart if data is available
            if 'received_free_data' in game_info and not game_info['received_free_data'].empty:
                pie_fig = create_acquisition_pie_chart(game_info)
                st.plotly_chart(pie_fig, use_container_width=True)
            else:
                st.info("No acquisition data available.")
        
        # Add a data table with the time series data
        with st.expander("View Daily Review Data"):
            # Sort from newest to oldest
            sorted_data = game_info['time_series_data'].sort_values(by='date', ascending=False)
            st.dataframe(sorted_data, use_container_width=True)
    elif not streaming:
        st.warning("Not enough time-based data available to create a timeline chart.")
        
    # Display sentiment review analysis
    if 'top_positive_reviews' in game_info and 'top_negative_reviews' in game_info:
        st.subheader("Sentiment Analysis Reviews")
        
        # Create tabs for positive and negative reviews
        pos_tab, neg_tab = st.tabs(["Most Upvoted Positive Reviews", "Most Upvoted Negative Reviews"])
        
        with pos_tab:
            if not game_info['top_positive_reviews'].empty:
                display_sentiment_reviews(game_info['top_positive_reviews'], sentiment_type="positive")
            else:
                st.info("No positive reviews found for analysis.")
                
        with neg_tab:
            if not game_info['top_negative_reviews'].empty:
                display_sentiment_reviews(game_info['top_negative_reviews'], sentiment_type="negative")
            else:
                st.info("No negative reviews found for analysis.")
    
    # Language filter for reviews (only if not in streaming mode)
    if not streaming and 'available_languages' in game_info:
        st.subheader("Filter Reviews by Language")
        
        # Create a dropdown with available languages
        available_langs = ["all"] + game_info.get("available_languages", [])
        
        # Create display names for the dropdown
        lang_options = [LANGUAGE_DISPLAY_NAMES.get(lang, lang) for lang in available_langs]
        
        # Create a mapping from display name back to language code
        display_to_lang = {LANGUAGE_DISPLAY_NAMES.get(lang, lang): lang for lang in available_langs}
        
        # Create the dropdown
        selected_lang_display = st.selectbox(
            "Select language for reviews:", 
            options=lang_options,
            index=0  # Default to "All Languages"
        )
        
        # Convert back to language code
        selected_language = display_to_lang.get(selected_lang_display, "all")
        
        # Filter and display reviews
        if 'reviews_sample' in game_info and not game_info['reviews_sample'].empty:
            filtered_reviews = filter_reviews_by_language(game_info['reviews_sample'], selected_language)
            
            st.subheader(f"Sample Reviews")
            if not filtered_reviews.empty:
                display_review_cards(filtered_reviews)
            else:
                st.info(f"No reviews found in the selected language.")

def render_game_info_tab(data_mode: str = "batch"):
    """Content for the Game Info tab"""
    st.subheader("Search for a specific game")
    
    # Game search input
    game_name = st.text_input("Enter a game name:", placeholder="e.g. Counter-Strike 2")
    
    # Show different content based on mode
    if data_mode == "batch":
        # Date range filter (only for batch mode)
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
        
        # Path to parquet files
        parquet_dir = "D:/STUFF/Projects/BigData_Project/data/all_reviews/cleaned_reviews"
        
        st.info("Batch Analysis Mode: Data will be analyzed directly from Parquet files")
        render_batch_mode(parquet_dir, game_name, start_datetime, end_datetime, show_debug)
    else:
        st.info("Streaming Mode: Data is from the real-time Kafka pipeline")
        render_streaming_mode(game_name) 