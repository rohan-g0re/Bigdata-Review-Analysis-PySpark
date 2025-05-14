import pandas as pd
import torch
import numpy as np
import streamlit as st
from transformers import AutoModelForSequenceClassification, AutoTokenizer

# Load model and tokenizer (with caching)
@st.cache_resource
def load_sentiment_model():
    """Load and cache the sentiment analysis model"""
    model = AutoModelForSequenceClassification.from_pretrained(
        "distilbert-base-uncased-finetuned-sst-2-english"
    )
    tokenizer = AutoTokenizer.from_pretrained(
        "distilbert-base-uncased-finetuned-sst-2-english"
    )
    return model, tokenizer

def analyze_sentiment_batch(texts, model, tokenizer, batch_size=32):
    """Process reviews in batches to analyze sentiment
    
    Args:
        texts: List of text strings to analyze
        model: Loaded sentiment model
        tokenizer: Loaded tokenizer
        batch_size: Number of texts to process in each batch
        
    Returns:
        List of dictionaries containing sentiment scores and confidence values
    """
    results = []
    
    for i in range(0, len(texts), batch_size):
        batch = texts[i:i+batch_size]
        
        # Tokenize inputs
        inputs = tokenizer(batch, truncation=True, padding=True, max_length=128, return_tensors="pt")
        
        # Move inputs to the same device as model
        inputs = {k: v.to(model.device) for k, v in inputs.items()}
        
        # Get predictions
        with torch.no_grad():
            outputs = model(**inputs)
        
        # Process outputs - for SST-2, index 0 is negative and index 1 is positive
        probabilities = torch.nn.functional.softmax(outputs.logits, dim=1)
        
        # Convert to sentiment scores (-1 to 1)
        for probs in probabilities:
            # Calculate sentiment as positive - negative probability
            sentiment_score = probs[1].item() - probs[0].item()
            confidence = max(probs[0].item(), probs[1].item())
            results.append({
                "sentiment": float(sentiment_score), 
                "confidence": float(confidence),
                "raw_scores": {
                    "negative": float(probs[0]),
                    "positive": float(probs[1]),
                }
            })
            
    return results

def get_overall_sentiment(sentiment_results):
    """Calculate overall sentiment score from individual results
    
    Args:
        sentiment_results: List of sentiment analysis results
        
    Returns:
        Float representing overall weighted sentiment score
    """
    if not sentiment_results:
        return 0.0
        
    # Weight by confidence
    weighted_sum = sum(r["sentiment"] * r["confidence"] for r in sentiment_results)
    total_confidence = sum(r["confidence"] for r in sentiment_results)
    
    if total_confidence == 0:
        return 0.0
        
    return weighted_sum / total_confidence

def get_extreme_reviews(reviews_df, sentiment_results, count=10, sentiment_type="positive"):
    """
    Get the most upvoted positive or negative reviews.
    
    Args:
        reviews_df: DataFrame with reviews
        sentiment_results: List of sentiment analysis results with same index as reviews_df
        count: Number of reviews to return
        sentiment_type: Either "positive" or "negative"
    
    Returns:
        DataFrame with top reviews sorted by upvotes
    """
    # Add sentiment scores to reviews
    reviews_with_sentiment = reviews_df.copy()
    reviews_with_sentiment["sentiment"] = [r["sentiment"] for r in sentiment_results]
    reviews_with_sentiment["confidence"] = [r["confidence"] for r in sentiment_results]
    
    # Filter reviews by sentiment type
    if sentiment_type == "positive":
        # Consider reviews with sentiment score >= 0.2 as positive
        filtered_reviews = reviews_with_sentiment[reviews_with_sentiment["sentiment"] >= 0.2]
    else:
        # Consider reviews with sentiment score <= -0.2 as negative
        filtered_reviews = reviews_with_sentiment[reviews_with_sentiment["sentiment"] <= -0.2]
    
    # If no reviews meet the criteria, return empty DataFrame
    if filtered_reviews.empty:
        return filtered_reviews
    
    # Sort the filtered reviews by upvotes (descending)
    sorted_reviews = filtered_reviews.sort_values("votes_up", ascending=False)
    
    # Return the top reviews
    return sorted_reviews.head(count) 