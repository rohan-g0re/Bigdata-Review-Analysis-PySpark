import streamlit as st
import plotly.express as px
from datetime import datetime, timedelta
from src.utils.spark_session import get_spark_session
from src.tabs.engagement_tab.analysis import get_all_engagement_metrics

def render_engagement_tab():
    """Content for the Engagement tab"""
    st.subheader("Steam Game Engagement Metrics")
    
    # Date range filter at the top
    st.subheader("Filter by Date Range")
    
    col1, col2 = st.columns(2)
    
    # Default date range (last 5 years)
    default_end_date = datetime.now()
    default_start_date = default_end_date - timedelta(days=365*5)
    
    with col1:
        start_date = st.date_input("Start Date", value=default_start_date, key="engagement_start_date")
    
    with col2:
        end_date = st.date_input("End Date", value=default_end_date, key="engagement_end_date")
    
    # Convert date inputs to datetime objects
    start_datetime = datetime.combine(start_date, datetime.min.time())
    end_datetime = datetime.combine(end_date, datetime.max.time())
    
    # Add note about date filter
    st.caption("Note: Date range filters reviews by their creation date")
    
    # Input for number of top games
    k = st.slider("Number of Top Games to Display", min_value=5, max_value=50, value=10, step=5)
    
    # Initialize session state for storing results
    if 'engagement_results' not in st.session_state:
        st.session_state.engagement_results = None
    
    # Path to parquet files
    parquet_dir = "D:/STUFF/Projects/BigData_Project/data/all_reviews/cleaned_reviews"
    
    # Run analysis when user clicks button
    if st.button("Run Engagement Analysis"):
        with st.spinner("Analyzing data... This may take a minute..."):
            # Get Spark session
            spark = get_spark_session(app_name="EngagementAnalysis")
            
            try:
                # Get all metrics in a single pass
                results = get_all_engagement_metrics(
                    spark, 
                    parquet_dir, 
                    games_k=k, 
                    authors_k=10,  # Always show top 10 authors
                    start_date=start_datetime, 
                    end_date=end_datetime
                )
                
                # Store results in session state
                st.session_state.engagement_results = results
                
            except Exception as e:
                st.error(f"Error during analysis: {e}")
            finally:
                # Stop Spark session
                spark.stop()
    
    # Display results if available
    if st.session_state.engagement_results is not None:
        results = st.session_state.engagement_results
        
        # Display top games results
        top_games = results["top_games"]
        if top_games is not None and not top_games.empty:
            # Display results
            st.subheader(f"Top {len(top_games)} Games by Review Count")
            st.caption(f"Data filtered from {start_date} to {end_date}")
            
            # Display as a table with custom column config to reduce width
            st.dataframe(
                top_games,
                column_config={
                    "game": st.column_config.TextColumn("Game", width="medium"),
                    "count": st.column_config.NumberColumn("Review Count", width="small")
                },
                hide_index=True,
                use_container_width=False
            )
            
            # Create bar chart with reduced width
            fig = px.bar(
                top_games,
                x="game",
                y="count",
                title=f"Top Games by Review Count",
                labels={"game": "Game Name", "count": "Number of Reviews"},
                color="game",
                text="count"
            )
            fig.update_layout(
                xaxis_tickangle=45,
                width=700,  # Set a fixed width
                height=500
            )
            st.plotly_chart(fig, use_container_width=False)
            
            # Show statistics
            st.subheader("Game Statistics")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Most Reviewed Game", top_games.iloc[0]["game"])
            with col2:
                st.metric("Most Reviews Count", f"{top_games.iloc[0]['count']:,}")
            with col3:
                avg_reviews = int(top_games["count"].mean())
                st.metric("Average Reviews (Top Games)", f"{avg_reviews:,}")
        
        # Author section - both tables are calculated, but display based on selection
        st.subheader("Author Leaderboards")
        
        # Dropdown for author leaderboard view selection
        author_view = st.selectbox(
            "Select View", 
            ["Review Count", "Review Upvotes"],
            index=0
        )
        
        # Display author leaderboard based on selection
        if author_view == "Review Count":
            top_authors = results["top_authors_by_count"]
            if top_authors is not None and not top_authors.empty:
                # Display leaderboard for top reviewers
                st.subheader("Top 10 Authors by Review Count")
                
                # Display as a table with custom column config to reduce width
                st.dataframe(
                    top_authors,
                    column_config={
                        "author_id": st.column_config.TextColumn("Author ID", width="medium"),
                        "review_count": st.column_config.NumberColumn("Review Count", width="small")
                    },
                    hide_index=True,
                    use_container_width=False
                )
                
                # Create bar chart with reduced width
                fig_reviewers = px.bar(
                    top_authors,
                    x="author_id",
                    y="review_count",
                    title="Top 10 Authors by Review Count",
                    labels={"author_id": "Author ID", "review_count": "Number of Reviews"},
                    color="author_id",
                    text="review_count"
                )
                fig_reviewers.update_layout(
                    xaxis_tickangle=45,
                    width=700,  # Set a fixed width
                    height=500
                )
                st.plotly_chart(fig_reviewers, use_container_width=False)
        else:  # "Review Upvotes"
            top_upvoted = results["top_authors_by_upvotes"]
            if top_upvoted is not None and not top_upvoted.empty:
                # Display leaderboard for most upvoted authors
                st.subheader("Top 10 Authors by Total Upvotes")
                
                # Display as a table with custom column config to reduce width
                st.dataframe(
                    top_upvoted,
                    column_config={
                        "author_id": st.column_config.TextColumn("Author ID", width="medium"),
                        "total_upvotes": st.column_config.NumberColumn("Total Upvotes", width="small"),
                        "review_count": st.column_config.NumberColumn("Review Count", width="small")
                    },
                    hide_index=True,
                    use_container_width=False
                )
                
                # Create bar chart with reduced width
                fig_upvoted = px.bar(
                    top_upvoted,
                    x="author_id",
                    y="total_upvotes",
                    title="Top 10 Authors by Total Upvotes",
                    labels={"author_id": "Author ID", "total_upvotes": "Total Upvotes"},
                    color="author_id",
                    text="total_upvotes"
                )
                fig_upvoted.update_layout(
                    xaxis_tickangle=45,
                    width=700,  # Set a fixed width
                    height=500
                )
                st.plotly_chart(fig_upvoted, use_container_width=False)
        
        # Add a note for additional context about author IDs
        with st.expander("Note on Author IDs"):
            st.markdown("""
            Author IDs shown here are Steam user identifiers. Both author leaderboard views (Review Count and Review Upvotes)
            are calculated in a single data pass, and you can switch between them using the dropdown above.
            """) 