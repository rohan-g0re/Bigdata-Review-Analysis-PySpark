import streamlit as st
import pandas as pd
from pyspark.sql.functions import col, to_date

def load_parquet_data(spark, parquet_dir):
    """
    Load data from a Parquet directory.
    
    Parameters:
    - spark: SparkSession object
    - parquet_dir: Path to the Parquet directory
    
    Returns:
    - Spark DataFrame with the loaded data
    """
    try:
        df = spark.read.parquet(parquet_dir)
        return df
    except Exception as e:
        st.error(f"Error loading Parquet data: {e}")
        return None

def apply_date_filter(df, start_date, end_date, date_col="timestamp_created"):
    """
    Filter a DataFrame by date range.
    
    Parameters:
    - df: Spark DataFrame to filter
    - start_date: Start date (datetime object)
    - end_date: End date (datetime object)
    - date_col: Column to use for date filtering
    
    Returns:
    - Filtered Spark DataFrame
    """
    try:
        # Add a date column for filtering
        filtered_df = df.withColumn("review_date", to_date(col(date_col)))
        
        # Convert Python datetime to string format YYYY-MM-DD for Spark SQL
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")
        
        # Filter by date range
        filtered_df = filtered_df.filter(
            (col("review_date") >= start_str) & (col("review_date") <= end_str)
        )
        
        return filtered_df
    except Exception as e:
        st.error(f"Error applying date filter: {e}")
        return df  # Return original DataFrame on error
        
def truncate_text(text, max_length=300):
    """Truncate text to specified length with ellipsis if needed."""
    if text and len(text) > max_length:
        return text[:max_length] + "..."
    return text 