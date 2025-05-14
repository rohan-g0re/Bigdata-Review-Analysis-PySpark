import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

def create_time_series_plot(game_info):
    """
    Create a time series plot for game reviews.
    
    Parameters:
    - game_info: Dictionary containing game data including time_series_data
    
    Returns:
    - Plotly figure object
    """
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
    
    return fig

def display_review_cards(reviews_df, vote_type="upvotes"):
    """
    Display review cards for a dataset of reviews.
    
    Parameters:
    - reviews_df: Pandas DataFrame containing review data
    - vote_type: Type of votes to display ("upvotes" or "funny")
    """
    vote_field = "votes_up" if vote_type == "upvotes" else "votes_funny"
    vote_icon = "👍" if vote_type == "upvotes" else "😂"
    vote_label = "upvotes" if vote_type == "upvotes" else "funny votes"
    
    for i, row in reviews_df.iterrows():
        with st.container():
            st.markdown(f"""
            <div style="border: 1px solid #ddd; padding: 10px; border-radius: 5px; margin-bottom: 10px;">
                <div style="display: flex; justify-content: space-between; margin-bottom: 5px;">
                    <span style="color: #888;">User: {row['author_id'][:8]}...</span>
                    <span style="color: #888;">Date: {row['date']}</span>
                </div>
                <div style="margin-bottom: 10px;">{row['review_text']}</div>
                <div style="display: flex; justify-content: space-between;">
                    <span>{vote_icon} <strong>{row[vote_field]}</strong> {vote_label}</span>
                    <span>⏱️ <strong>{row['playtime_hrs']}</strong> hours played</span>
                </div>
            </div>
            """, unsafe_allow_html=True) 