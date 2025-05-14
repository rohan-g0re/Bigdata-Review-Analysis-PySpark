import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import numpy as np

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

def create_acquisition_pie_chart(game_info):
    """
    Create a pie chart showing free vs. purchased game acquisition.
    
    Parameters:
    - game_info: Dictionary containing game data including received_free_data
    
    Returns:
    - Plotly figure object
    """
    # Create pie chart
    fig = px.pie(
        game_info['received_free_data'],
        values='count',
        names='category',
        title=f"How Users Acquired {game_info['game_name']}",
        color='category',
        color_discrete_map={'Purchased': 'rgba(0, 176, 246, 0.7)', 'Free': 'rgba(246, 78, 139, 0.7)'}
    )
    
    # Update layout for better readability
    fig.update_layout(
        legend_title="Acquisition Method"
    )
    
    # Update traces for better hover info
    fig.update_traces(
        textinfo='percent+value',
        hoverinfo='label+percent+value'
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

def create_sentiment_gauge(sentiment_score):
    """
    Create a gauge chart for visualizing overall sentiment score.
    
    Parameters:
    - sentiment_score: Float between -1 and 1 representing sentiment
    
    Returns:
    - Plotly figure object
    """
    # Ensure the score is within bounds
    sentiment_score = max(min(sentiment_score, 1.0), -1.0)
    
    # Create a clear label for the sentiment
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
    
    # Create gauge chart
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=sentiment_score,
        domain={'x': [0, 1], 'y': [0, 1]},
        title={'text': f"Overall Sentiment: {sentiment_label}"},
        gauge={
            'axis': {'range': [-1, 1], 'tickmode': 'array', 'tickvals': [-1, -0.6, -0.2, 0.2, 0.6, 1],
                     'ticktext': ['Very<br>Negative', 'Negative', 'Mixed', 'Positive', 'Very<br>Positive', '']},
            'bar': {'color': get_sentiment_color(sentiment_score)},
            'steps': [
                {'range': [-1, -0.6], 'color': "rgba(220, 20, 60, 0.6)"},  # Crimson (negative)
                {'range': [-0.6, -0.2], 'color': "rgba(255, 165, 0, 0.6)"}, # Orange (somewhat negative)
                {'range': [-0.2, 0.2], 'color': "rgba(255, 215, 0, 0.6)"},  # Gold (neutral)
                {'range': [0.2, 0.6], 'color': "rgba(154, 205, 50, 0.6)"},  # Yellow-green (somewhat positive) 
                {'range': [0.6, 1], 'color': "rgba(34, 139, 34, 0.6)"}      # Forest green (positive)
            ],
            'threshold': {
                'line': {'color': "black", 'width': 2},
                'thickness': 0.75,
                'value': sentiment_score
            }
        }
    ))
    
    # Improve layout
    fig.update_layout(
        height=300,
        font={'size': 14},
        margin=dict(l=20, r=20, t=40, b=20)
    )
    
    return fig

def get_sentiment_color(sentiment_score):
    """
    Get an appropriate color for the given sentiment score.
    
    Parameters:
    - sentiment_score: Float between -1 and 1
    
    Returns:
    - String with rgba color
    """
    # Map sentiment to a color
    if sentiment_score >= 0.6:
        return "rgba(34, 139, 34, 1)"       # Forest green (very positive)
    elif sentiment_score >= 0.2:
        return "rgba(154, 205, 50, 1)"      # Yellow-green (positive)
    elif sentiment_score >= -0.2:
        return "rgba(255, 215, 0, 1)"       # Gold (neutral)
    elif sentiment_score >= -0.6:
        return "rgba(255, 165, 0, 1)"       # Orange (negative)
    else:
        return "rgba(220, 20, 60, 1)"       # Crimson (very negative)

def display_sentiment_reviews(reviews_df, sentiment_type="positive"):
    """
    Display sentiment-analyzed review cards with appropriate styling.
    
    Parameters:
    - reviews_df: Pandas DataFrame containing review data with sentiment scores
    - sentiment_type: Either "positive" or "negative"
    """
    if reviews_df.empty:
        st.info(f"No {sentiment_type} reviews available.")
        return
    
    for i, row in reviews_df.iterrows():
        # Determine colors based on sentiment score
        sentiment_score = row.get('sentiment', 0)
        border_color = get_sentiment_color(sentiment_score)
        bg_color = border_color.replace("1)", "0.1)")
        
        # Determine emoji based on sentiment type
        if sentiment_type == "positive":
            sentiment_emoji = "😄"
        else:
            sentiment_emoji = "😞"
        
        # Format sentiment score for display
        sentiment_value = f"{sentiment_score:.2f}"
        confidence = f"{row.get('confidence', 0):.2f}"
        
        with st.container():
            st.markdown(f"""
            <div style="border: 1px solid {border_color}; padding: 10px; border-radius: 5px; margin-bottom: 10px; background-color: {bg_color};">
                <div style="display: flex; justify-content: space-between; margin-bottom: 5px;">
                    <span style="color: #888;">User: {row['author_id'][:8]}...</span>
                    <span style="color: #888;">Date: {row['date']}</span>
                </div>
                <div style="margin-bottom: 10px;">{row['review_text']}</div>
                <div style="display: flex; justify-content: space-between;">
                    <span>{sentiment_emoji} <strong>Sentiment:</strong> {sentiment_value} (Confidence: {confidence})</span>
                    <span>⏱️ <strong>{row['playtime_hrs']}</strong> hours played</span>
                </div>
            </div>
            """, unsafe_allow_html=True)

def create_sentiment_distribution_chart(sentiment_results):
    """
    Create a histogram showing the distribution of sentiment scores.
    
    Parameters:
    - sentiment_results: List of sentiment results from sentiment analysis
    
    Returns:
    - Plotly figure object
    """
    if not sentiment_results:
        return None
    
    # Extract sentiment scores
    sentiment_scores = [result['sentiment'] for result in sentiment_results]
    
    # Create histogram bins from -1 to 1
    bins = np.linspace(-1, 1, 21)  # 20 bins
    
    # Create histogram
    fig = px.histogram(
        x=sentiment_scores,
        nbins=20,
        range_x=[-1, 1],
        title="Distribution of Sentiment Scores",
        labels={"x": "Sentiment Score", "y": "Number of Reviews"},
        color_discrete_sequence=['rgba(0, 176, 246, 0.7)']
    )
    
    # Add vertical line at sentiment=0
    fig.add_vline(
        x=0, 
        line_dash="dash", 
        line_color="rgba(0, 0, 0, 0.5)",
        annotation_text="Neutral",
        annotation_position="top"
    )
    
    # Improve layout
    fig.update_layout(
        xaxis=dict(
            tickmode='array',
            tickvals=[-1, -0.5, 0, 0.5, 1],
            ticktext=['Very Negative', 'Negative', 'Neutral', 'Positive', 'Very Positive']
        ),
        bargap=0.1
    )
    
    return fig 