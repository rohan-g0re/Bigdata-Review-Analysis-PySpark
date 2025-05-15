import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import pandas as pd
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
            # Show statistics ABOVE the treemap
            st.subheader("Game Statistics")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Most Reviewed Game", top_games.iloc[0]["game"])
            with col2:
                st.metric("Most Reviews Count", f"{top_games.iloc[0]['count']:,}")
            with col3:
                avg_reviews = int(top_games["count"].mean())
                st.metric("Average Reviews (Top Games)", f"{avg_reviews:,}")
            
            # Display treemap below the statistics
            st.subheader(f"Top {len(top_games)} Games by Review Count")
            
            # Create treemap
            fig_treemap = px.treemap(
                top_games,
                path=["game"],
                values="count",
                color="count",
                color_continuous_scale="Viridis",
                hover_data=["game", "count"]
            )
            fig_treemap.update_layout(
                height=600,  # Increased height for better visibility
                font=dict(size=16)  # Increase base font size throughout the figure
            )
            fig_treemap.update_traces(
                textinfo="label+value+percent root",
                hovertemplate='<b>%{label}</b><br>Reviews: %{value}<br>Percentage: %{percentRoot:.2%}<extra></extra>',
                textfont=dict(size=18),  # Increase font size for text inside the treemap boxes
                insidetextfont=dict(size=18),  # Increase font size for inside labels
                outsidetextfont=dict(size=18)  # Increase font size for outside labels
            )
            st.plotly_chart(fig_treemap, use_container_width=True)
        
        # Consolidated "Most Influential Authors" section
        st.subheader("Most Influential Authors")
        
        # Combine both datasets (by count and by upvotes)
        top_authors_by_count = results["top_authors_by_count"]
        top_authors_by_upvotes = results["top_authors_by_upvotes"]
        
        if (top_authors_by_count is not None and not top_authors_by_count.empty and 
            top_authors_by_upvotes is not None and not top_authors_by_upvotes.empty):
            
            # Create a combined dataframe with influence metrics
            combined_authors = pd.merge(
                top_authors_by_count, 
                top_authors_by_upvotes[["author_id", "total_upvotes"]], 
                on="author_id", 
                how="outer"
            ).fillna(0)
            
            # Calculate additional metrics
            combined_authors["upvotes_per_review"] = combined_authors["total_upvotes"] / combined_authors["review_count"]
            combined_authors["upvotes_per_review"] = combined_authors["upvotes_per_review"].fillna(0).round(2)
            
            # Calculate influence score (normalized combination of review count and upvotes)
            combined_authors["influence_score"] = (
                0.4 * (combined_authors["review_count"] / combined_authors["review_count"].max()) + 
                0.6 * (combined_authors["total_upvotes"] / combined_authors["total_upvotes"].max())
            ).round(2) * 100  # Scale to 0-100
            
            # Sort by influence score
            combined_authors = combined_authors.sort_values("influence_score", ascending=False).head(10)
            
            # Display as a table
            st.dataframe(
                combined_authors,
                column_config={
                    "author_id": st.column_config.TextColumn("Author ID", width="medium"),
                    "review_count": st.column_config.NumberColumn("Reviews", width="small"),
                    "total_upvotes": st.column_config.NumberColumn("Total Upvotes", width="small"),
                    "upvotes_per_review": st.column_config.NumberColumn("Upvotes/Review", width="small"),
                    "influence_score": st.column_config.ProgressColumn(
                        "Influence Score",
                        width="medium",
                        format="%d",
                        min_value=0,
                        max_value=100
                    )
                },
                hide_index=True,
                use_container_width=True
            )
            
            # Increase font size for table headers and content
            st.markdown("""
            <style>
            .stDataFrame {
                font-size: 18px !important;
            }
            .stDataFrame th {
                font-size: 20px !important;
                font-weight: bold !important;
            }
            .stDataFrame td {
                font-size: 18px !important;
            }
            </style>
            """, unsafe_allow_html=True)
            
            # Create lollipop chart for author influence visualization
            fig_lollipop = go.Figure()
            
            # Sort for better visualization
            chart_data = combined_authors.sort_values("influence_score")
            
            # Add lines (stems)
            fig_lollipop.add_trace(go.Scatter(
                x=chart_data["influence_score"],
                y=chart_data["author_id"],
                mode='lines',
                line=dict(color='rgba(0, 0, 0, 0.3)', width=1),
                showlegend=False
            ))
            
            # Add markers (lollipops)
            fig_lollipop.add_trace(go.Scatter(
                x=chart_data["influence_score"],
                y=chart_data["author_id"],
                mode='markers',
                marker=dict(
                    color='rgba(58, 71, 180, 0.8)',
                    size=12,
                    line=dict(color='rgba(0, 0, 0, 0.5)', width=1)
                ),
                text=[
                    f"Author: {row['author_id']}<br>" +
                    f"Influence Score: {row['influence_score']}<br>" +
                    f"Reviews: {row['review_count']}<br>" +
                    f"Total Upvotes: {row['total_upvotes']}"
                    for _, row in chart_data.iterrows()
                ],
                hoverinfo='text',
                showlegend=False
            ))
            
            fig_lollipop.update_layout(
                title="Author Influence Distribution",
                xaxis_title="Influence Score",
                yaxis_title="Author ID",
                height=500,
                margin=dict(l=10, r=10, t=40, b=10),
                xaxis=dict(range=[0, 105]),  # Allow some space at the right
                font=dict(size=18),  # Increase base font size for the chart
                title_font=dict(size=22)  # Increase title font size
            )
            
            # Increase font size for axis titles
            fig_lollipop.update_xaxes(title_font=dict(size=20))
            fig_lollipop.update_yaxes(title_font=dict(size=20))
            
            st.plotly_chart(fig_lollipop, use_container_width=True) 