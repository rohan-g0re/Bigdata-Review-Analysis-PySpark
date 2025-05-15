import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import altair as alt
import pandas as pd

def create_time_series_plot(game_info):
    """
    Create a time series plot for game reviews using Altair.
    
    Parameters:
    - game_info: Dictionary containing game data including time_series_data
    
    Returns:
    - Altair chart object
    """
    # Prepare data
    df = game_info['time_series_data']
    
    # Create hover selection
    hover = alt.selection_point(
        fields=["date"],
        nearest=True,
        on="mouseover",
        empty=False,
    )
    
    # Base line chart
    line = alt.Chart(df).mark_line(
        color='#1E88E5',
        strokeWidth=2
    ).encode(
        x=alt.X('date:T', title='Date', axis=alt.Axis(format='%b %Y', labelAngle=-45)),
        y=alt.Y('count:Q', title='Number of Reviews')
    )
    
    # Create a transparent overlay for tooltips
    points = line.mark_point(
        size=100,
        opacity=0,
        filled=True
    ).encode(
        tooltip=[
            alt.Tooltip('date:T', title='Date', format='%b %d, %Y'),
            alt.Tooltip('count:Q', title='Reviews')
        ]
    ).add_selection(hover)
    
    # Draw points on hover
    tooltips = line.mark_point(
        size=100,
        filled=True,
        color='#1E88E5'
    ).encode(
        opacity=alt.condition(hover, alt.value(1), alt.value(0))
    )
    
    # Create a rule to connect points
    rule = alt.Chart(df).mark_rule(
        color='gray',
        strokeWidth=1,
        strokeDash=[5, 5]
    ).encode(
        x='date:T'
    ).transform_filter(hover)
    
    # Add an area below the line for better visuals
    area = alt.Chart(df).mark_area(
        opacity=0.2,
        color='#1E88E5'
    ).encode(
        x='date:T',
        y='count:Q'
    )
    
    # Combine all parts to create the full chart
    chart = alt.layer(
        area, line, points, tooltips, rule
    ).properties(
        width='container',
        height=350,
        title=f"Daily Review Count for {game_info['game_name']}"
    ).configure_title(
        fontSize=16,
        font='Segoe UI',
        anchor='start',
        color='#1E3A8A'
    ).configure_view(
        strokeWidth=0
    )
    
    return chart

def create_acquisition_pie_chart(game_info):
    """
    Create a pie chart showing free vs. purchased game acquisition using Altair.
    
    Parameters:
    - game_info: Dictionary containing game data including received_free_data
    
    Returns:
    - Altair chart object
    """
    # Prepare data
    df = game_info['received_free_data']
    
    # Create a pie chart with Altair
    chart = alt.Chart(df).mark_arc(innerRadius=50).encode(
        theta=alt.Theta(field="count", type="quantitative"),
        color=alt.Color(
            field="category", 
            type="nominal",
            scale=alt.Scale(
                domain=['Purchased', 'Free'],
                range=['#1E88E5', '#F06292']
            ),
            legend=alt.Legend(title="Acquisition Method")
        ),
        tooltip=[
            alt.Tooltip("category:N", title="Method"),
            alt.Tooltip("count:Q", title="Users"),
            alt.Tooltip("percent:Q", title="Percentage")
        ]
    ).transform_calculate(
        # Calculate the percentage for tooltips
        percent="datum.count / sum(datum.count) * 100"
    ).properties(
        width='container',
        height=300,
        title=f"How Users Acquired {game_info['game_name']}"
    ).configure_title(
        fontSize=16,
        font='Segoe UI',
        anchor='start',
        color='#1E3A8A'
    ).configure_view(
        strokeWidth=0
    )
    
    return chart

def display_review_cards(reviews_df, vote_type="upvotes"):
    """
    Display review cards for a dataset of reviews with modern styling.
    
    Parameters:
    - reviews_df: Pandas DataFrame containing review data
    - vote_type: Type of votes to display ("upvotes", "funny", or "comments")
    """
    if vote_type == "upvotes":
        vote_field = "votes_up"
        vote_icon = "👍"
        vote_label = "upvotes"
        card_border = "#1E88E5"  # Blue for upvotes
    elif vote_type == "funny":
        vote_field = "votes_funny"
        vote_icon = "😂"
        vote_label = "funny votes"
        card_border = "#9C27B0"  # Purple for funny
    elif vote_type == "comments":
        vote_field = "comment_count"
        vote_icon = "💬"
        vote_label = "comments"
        card_border = "#FF9800"  # Orange for comments
    
    # CSS for cards with modern styling
    st.markdown(f"""
    <style>
        .review-card {{
            border-radius: 8px;
            background-color: white;
            padding: 16px;
            margin-bottom: 16px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            border-left: 4px solid {card_border};
            transition: transform 0.2s ease, box-shadow 0.2s ease;
        }}
        .review-card:hover {{
            transform: translateY(-3px);
            box-shadow: 0 6px 12px rgba(0, 0, 0, 0.15);
        }}
        .review-header {{
            display: flex;
            justify-content: space-between;
            color: #64748B;
            font-size: 0.9em;
            margin-bottom: 12px;
            border-bottom: 1px solid #f1f5f9;
            padding-bottom: 8px;
        }}
        .review-content {{
            color: #1E293B;
            line-height: 1.6;
            margin-bottom: 12px;
            font-size: 1em;
        }}
        .review-footer {{
            display: flex;
            justify-content: space-between;
            color: #64748B;
            font-size: 0.9em;
            padding-top: 8px;
            border-top: 1px solid #f1f5f9;
        }}
        .review-stats {{
            display: flex;
            gap: 16px;
            align-items: center;
        }}
        .stat-value {{
            color: #334155;
            font-weight: 600;
        }}
    </style>
    """, unsafe_allow_html=True)
    
    for i, row in reviews_df.iterrows():
        with st.container():
            st.markdown(f"""
            <div class="review-card">
                <div class="review-header">
                    <span>User: {row['author_id'][:8]}...</span>
                    <span>Date: {row['date']}</span>
                </div>
                <div class="review-content">{row['review_text']}</div>
                <div class="review-footer">
                    <div class="review-stats">
                        <span>{vote_icon} <span class="stat-value">{row[vote_field]}</span> {vote_label}</span>
                    </div>
                    <div class="review-stats">
                        <span>⏱️ <span class="stat-value">{row['playtime_hrs']}</span> hours played</span>
                    </div>
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
    Display sentiment-analyzed review cards with modern styling.
    
    Parameters:
    - reviews_df: Pandas DataFrame containing review data with sentiment scores
    - sentiment_type: Either "positive" or "negative"
    """
    if reviews_df.empty:
        st.info(f"No {sentiment_type} reviews available.")
        return
    
    # Apply consistent styling for sentiment cards
    # Border color will be based on the sentiment of each individual review
    st.markdown("""
    <style>
        .sentiment-card {
            border-radius: 8px;
            background-color: white;
            padding: 16px;
            margin-bottom: 16px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            transition: transform 0.2s ease, box-shadow 0.2s ease;
        }
        .sentiment-card:hover {
            transform: translateY(-3px);
            box-shadow: 0 6px 12px rgba(0, 0, 0, 0.15);
        }
        .sentiment-header {
            display: flex;
            justify-content: space-between;
            color: #64748B;
            font-size: 0.9em;
            margin-bottom: 12px;
            border-bottom: 1px solid #f1f5f9;
            padding-bottom: 8px;
        }
        .sentiment-content {
            color: #1E293B;
            line-height: 1.6;
            margin-bottom: 12px;
            font-size: 1em;
        }
        .sentiment-footer {
            display: flex;
            justify-content: space-between;
            color: #64748B;
            font-size: 0.9em;
            padding-top: 8px;
            border-top: 1px solid #f1f5f9;
        }
        .sentiment-stats {
            display: flex;
            gap: 16px;
            align-items: center;
        }
        .stat-value {
            color: #334155;
            font-weight: 600;
        }
    </style>
    """, unsafe_allow_html=True)
    
    for i, row in reviews_df.iterrows():
        # Determine colors based on sentiment score
        sentiment_score = row.get('sentiment', 0)
        border_color = get_sentiment_color(sentiment_score)
        bg_color = border_color.replace("1)", "0.05)")
        
        # Determine emoji based on sentiment type
        if sentiment_type == "positive":
            sentiment_emoji = "😄"
        else:
            sentiment_emoji = "😞"
        
        # Format sentiment score for display
        sentiment_value = f"{sentiment_score:.2f}"
        confidence = f"{row.get('confidence', 0):.2f}"
        
        # Get upvote count, default to 0 if not present
        upvotes = row.get('votes_up', 0)
        
        with st.container():
            st.markdown(f"""
            <div class="sentiment-card" style="border-left: 4px solid {border_color}; background-color: {bg_color}">
                <div class="sentiment-header">
                    <span>User: {row['author_id'][:8]}...</span>
                    <span>Date: {row['date']}</span>
                </div>
                <div class="sentiment-content">{row['review_text']}</div>
                <div class="sentiment-footer">
                    <div class="sentiment-stats">
                        <span>{sentiment_emoji} <span class="stat-value">Sentiment:</span> {sentiment_value} (Confidence: {confidence})</span>
                    </div>
                    <div class="sentiment-stats">
                        <span>👍 <span class="stat-value">{upvotes}</span> upvotes</span>
                        <span>⏱️ <span class="stat-value">{row['playtime_hrs']}</span> hours played</span>
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)

def create_sentiment_distribution_chart(sentiment_results):
    """
    Create a histogram showing the distribution of sentiment scores using Altair.
    
    Parameters:
    - sentiment_results: List of sentiment results from sentiment analysis
    
    Returns:
    - Altair chart object
    """
    if not sentiment_results:
        return None
    
    # Extract sentiment scores and create a DataFrame
    sentiment_scores = [result['sentiment'] for result in sentiment_results]
    df = pd.DataFrame({'sentiment': sentiment_scores})
    
    # Create histogram
    base = alt.Chart(df).mark_bar(
        color='#1E88E5',
        cornerRadiusTopLeft=3,
        cornerRadiusTopRight=3
    ).encode(
        x=alt.X(
            'sentiment:Q',
            bin=alt.Bin(maxbins=20, extent=[-1, 1]),
            title='Sentiment Score',
            axis=alt.Axis(
                labelExpr="datum.value == -1 ? 'Very Negative' : datum.value == -0.5 ? 'Negative' : datum.value == 0 ? 'Neutral' : datum.value == 0.5 ? 'Positive' : datum.value == 1 ? 'Very Positive' : ''"
            )
        ),
        y=alt.Y(
            'count()',
            title='Number of Reviews'
        ),
        tooltip=[
            alt.Tooltip('count()', title='Reviews'),
            alt.Tooltip('sentiment:Q', bin=alt.Bin(maxbins=20), title='Sentiment Range')
        ]
    )
    
    # Add a vertical line for the neutral point
    rule = alt.Chart(pd.DataFrame({'x': [0]})).mark_rule(
        strokeDash=[4, 4],
        size=1,
        color='#666'
    ).encode(
        x='x:Q'
    )
    
    # Add text annotation for the neutral line
    text = alt.Chart(pd.DataFrame({'x': [0], 'y': [0], 'text': ['Neutral']})).mark_text(
        align='center',
        baseline='top',
        dy=-5,
        fontSize=12,
        font='Segoe UI'
    ).encode(
        x='x:Q',
        y=alt.Y('y:Q', title=''),
        text='text:N'
    )
    
    # Combine the chart elements
    chart = (base + rule + text).properties(
        width='container',
        height=300,
        title='Distribution of Sentiment Scores'
    ).configure_title(
        fontSize=16,
        font='Segoe UI',
        anchor='start',
        color='#1E3A8A'
    ).configure_view(
        strokeWidth=0
    )
    
    return chart 