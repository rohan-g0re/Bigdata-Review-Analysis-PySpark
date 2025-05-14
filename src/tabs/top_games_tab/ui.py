import streamlit as st
import plotly.express as px
from src.utils.spark_session import get_spark_session
from src.tabs.top_games_tab.analysis import get_top_k_games_by_reviews

def render_top_games_tab():
    """Content for the Top Games tab"""
    st.subheader("Discover the most reviewed games on Steam")
    
    # Input for number of top games
    k = st.slider("Number of Top Games to Display", min_value=5, max_value=50, value=10, step=5)
    
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
            except Exception as e:
                st.error(f"Error during analysis: {e}")
            finally:
                # Stop Spark session
                spark.stop() 