# Steam Game Reviews Analysis Dashboard

A data analysis dashboard for exploring Steam game reviews built with PySpark and Streamlit.

## Features

- **Top Games Tab**: Shows games with the most reviews
- **Game Info Tab**: Analyze specific games with:
  - Date range filtering
  - Time series visualization of review counts
  - Top reviews by upvotes, funny votes, and comment count
  - Language filtering for reviews
  - Pie chart showing free vs. purchased game acquisition
  - Early access review statistics
  - Sentiment analysis of reviews

## Running the Application

Data Directory: \data\all_reviews\cleaned_reviews

where, 
cleaned_reviews contains parquet files (crc files removed)

Run in PowerShell using: 
```
python -m streamlit run .\src\streamlit_app.py
```

## Requirements

See requirements.txt for dependencies.
