import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import altair as alt
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
        start_date = st.date_input("Start Date", value=default_start_date, key="game_start_date")
    
    with col2:
        end_date = st.date_input("End Date", value=default_end_date, key="game_end_date")
    
    # Convert date inputs to datetime objects
    start_datetime = datetime.combine(start_date, datetime.min.time())
    end_datetime = datetime.combine(end_date, datetime.max.time())
    
    # Add note about date filter
    st.caption("Note: Date range filters reviews by their creation date")
    
    # Add a debug toggle
    show_debug = st.checkbox("Show debug information")
    
    # Store game data in session state to avoid reloading
    if 'game_data' not in st.session_state:
        st.session_state.game_data = None
    
    if st.button("Search Game") and game_name:
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
        game_info = st.session_state.game_data
        
        # Create a nice card-like display
        st.markdown("---")
        st.markdown(f"## {game_info['game_name']}")
        st.caption(f"Data filtered from {start_date} to {end_date}")
        
        # Two vertical sections as requested by the user
        st.markdown("## Review Analysis")
        
        # === REVIEW ANALYSIS SECTION ===
        
        # Use columns for the key metrics - total reviews card and avg playtime
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
        
        # Add time series plot (line chart of reviews)
        if 'time_series_data' in game_info and not game_info['time_series_data'].empty:
            st.subheader("Reviews Over Time")
            
            # Create and display time series plot
            time_fig = create_time_series_plot(game_info)
            st.altair_chart(time_fig, use_container_width=True)
        else:
            st.warning("Not enough time-based data available to create a timeline chart.")
            
        # Pie chart of acquisition (keeping the chart but removing the heading)
        if 'received_free_data' in game_info and not game_info['received_free_data'].empty:
            pie_fig = create_acquisition_pie_chart(game_info)
            st.altair_chart(pie_fig, use_container_width=True)
        else:
            st.info("No acquisition data available.")
            
        # ADD BACK: Client-side filtering for reviews by language and review type
        if 'all_upvoted_reviews' in game_info or 'all_funny_reviews' in game_info or 'all_commented_reviews' in game_info:
            # Filter the reviews by language
            upvoted_reviews = filter_reviews_by_language(game_info.get('all_upvoted_reviews', pd.DataFrame()), "all")  # Default all languages
            funny_reviews = filter_reviews_by_language(game_info.get('all_funny_reviews', pd.DataFrame()), "all")
            commented_reviews = filter_reviews_by_language(game_info.get('all_commented_reviews', pd.DataFrame()), "all")
            
            # Create a subheader for the review section
            st.subheader("Browse Game Reviews")
            
            # Create side-by-side dropdowns
            left_col, right_col = st.columns(2)
            
            with left_col:
                # Determine available review types
                review_types = []
                if 'all_upvoted_reviews' in game_info and not game_info.get('all_upvoted_reviews', pd.DataFrame()).empty:
                    review_types.append("Most Upvoted Reviews")
                if 'all_funny_reviews' in game_info and not game_info.get('all_funny_reviews', pd.DataFrame()).empty:
                    review_types.append("Funniest Reviews")
                if 'all_commented_reviews' in game_info and not game_info.get('all_commented_reviews', pd.DataFrame()).empty:
                    review_types.append("Most Commented on Reviews")
                    
                if not review_types:
                    st.info("No reviews available.")
                    review_type = None
                else:
                    review_type = st.selectbox(
                        "Select review type:",
                        options=review_types,
                        index=0
                    )
            
            with right_col:
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
            
            # Filter reviews based on selected language
            if review_type and selected_language:
                upvoted_reviews = filter_reviews_by_language(game_info.get('all_upvoted_reviews', pd.DataFrame()), selected_language)
                funny_reviews = filter_reviews_by_language(game_info.get('all_funny_reviews', pd.DataFrame()), selected_language)
                commented_reviews = filter_reviews_by_language(game_info.get('all_commented_reviews', pd.DataFrame()), selected_language)
                
                # # Apply CSS for scrollable container
                # st.markdown("""
                # <style>
                #     .review-scroll-container {
                #         height: 500px;
                #         overflow-y: auto;
                #         padding: 1rem;
                #         border: 1px solid #e6e6e6;
                #         border-radius: 0.5rem;
                #     }
                # </style>
                # """, unsafe_allow_html=True)
                
                # Display the appropriate reviews in the scrollable container based on selection
                st.markdown('<div class="review-scroll-container">', unsafe_allow_html=True)
                
                if review_type == "Most Upvoted Reviews":
                    if not upvoted_reviews.empty:
                        for i, row in upvoted_reviews.iterrows():
                            display_review_cards(pd.DataFrame([row]), vote_type="upvotes")
                    else:
                        st.info(f"No upvoted reviews available in {selected_lang_display}.")
                        
                elif review_type == "Funniest Reviews":
                    if not funny_reviews.empty:
                        for i, row in funny_reviews.iterrows():
                            display_review_cards(pd.DataFrame([row]), vote_type="funny")
                    else:
                        st.info(f"No funny reviews available in {selected_lang_display}.")
                        
                else:  # Most Commented on Reviews
                    if not commented_reviews.empty:
                        for i, row in commented_reviews.iterrows():
                            display_review_cards(pd.DataFrame([row]), vote_type="comments")
                    else:
                        st.info(f"No reviews with comments available in {selected_lang_display}.")
                
                st.markdown('</div>', unsafe_allow_html=True)
            
        # === SENTIMENT ANALYSIS SECTION ===
        
        st.markdown("---")
        st.markdown("## Sentiment Analysis")
        
        # Only show sentiment section if available
        if 'sentiment_score' in game_info:
            # Display sentiment score card
            col1, col2 = st.columns([1, 2])
            
            with col1:
                # Sentiment score card
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
                
                # Gauge visualization
                gauge_fig = create_sentiment_gauge(game_info['sentiment_score'])
                st.plotly_chart(gauge_fig, use_container_width=True)
            
            with col2:
                # Text block explaining what the score means (not a dropdown)
                st.subheader("What This Score Means")
                st.markdown(f"""
                The game has an overall sentiment score of **{sentiment_score:.2f}** which is rated as **{sentiment_label}**.
                
                This score is calculated from analyzing {game_info.get('sentiment_analyzed_count', 0):,} reviews.
                
                **Sentiment Score Scale:**
                - **> 0.6**: Very Positive - Players strongly enjoy the game
                - **0.2 to 0.6**: Positive - Players generally like the game
                - **-0.2 to 0.2**: Mixed - Players have varied opinions
                - **-0.6 to -0.2**: Negative - Players generally dislike the game
                - **< -0.6**: Very Negative - Players strongly dislike the game
                
                This analysis evaluates the emotional tone of each review using AI language models.
                """)
        
            # Most upvoted positive and negative reviews
            st.subheader("Most Upvoted Reviews by Sentiment")
            
            pos_col, neg_col = st.columns(2)
            
            with pos_col:
                st.subheader("Most Upvoted Positive Reviews")
                if 'top_positive_reviews' in game_info and not game_info['top_positive_reviews'].empty:
                    # Filter by language if needed
                    filtered_pos = filter_reviews_by_language(game_info['top_positive_reviews'], selected_language)
                    if not filtered_pos.empty:
                        display_sentiment_reviews(filtered_pos, sentiment_type="positive")
                    else:
                        st.info(f"No positive reviews found in {selected_lang_display}.")
                else:
                    st.info("No positive reviews found for analysis.")
                    
            with neg_col:
                st.subheader("Most Upvoted Negative Reviews")
                if 'top_negative_reviews' in game_info and not game_info['top_negative_reviews'].empty:
                    # Filter by language if needed
                    filtered_neg = filter_reviews_by_language(game_info['top_negative_reviews'], selected_language)
                    if not filtered_neg.empty:
                        display_sentiment_reviews(filtered_neg, sentiment_type="negative")
                    else:
                        st.info(f"No negative reviews found in {selected_lang_display}.")
                else:
                    st.info("No negative reviews found for analysis.")
        else:
            st.info("Sentiment analysis is not available for this game.") 