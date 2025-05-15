"""
Script to test reading data from Parquet files
This demonstrates the producer's ability to fetch and process data
"""
import os
import json
import time
import pandas as pd
from typing import Dict, Any, List

# Import our Parquet reader
from src.streaming.producer.parquet_reader import ParquetReader
from src.streaming.config.settings import PARQUET_DIR, BATCH_SIZE

def test_parquet_reading(
    num_records: int = 100,
    parquet_dir: str = PARQUET_DIR,
    display_limit: int = 10
):
    """
    Test reading records from Parquet files
    
    Args:
        num_records: Number of records to read
        parquet_dir: Directory containing Parquet files
        display_limit: Maximum number of records to display fully
    """
    print(f"=== Testing Parquet Reader with {num_records} records ===\n")
    
    # Initialize reader
    reader = ParquetReader(parquet_dir=parquet_dir, batch_size=BATCH_SIZE)
    
    # Show file info
    print("Found Parquet files:")
    for i, file_path in enumerate(reader.files[:5]):
        size_mb = os.path.getsize(file_path) / (1024 * 1024)
        print(f"  {i+1}. {os.path.basename(file_path)} - {size_mb:.2f} MB")
    
    if len(reader.files) > 5:
        print(f"  ... and {len(reader.files) - 5} more files")
    
    print(f"\nTotal: {len(reader.files)} files, {reader.total_size_bytes / (1024**2):.2f} MB")
    
    # Read schema
    schema = reader.read_schema()
    print("\nSchema (column names):")
    print(", ".join(schema.names))
    
    # Read sample records
    print(f"\nReading {num_records} records...")
    
    # Since read_sample only reads from one file, we'll use read_all_batches
    all_records = []
    source_files = set()
    start_time = time.time()
    
    for batch, file_path in reader.read_all_batches(show_progress=False):
        source_files.add(os.path.basename(file_path))
        all_records.extend(batch)
        
        # Stop once we have enough records
        if len(all_records) >= num_records:
            all_records = all_records[:num_records]
            break
    
    end_time = time.time()
    
    print(f"Read {len(all_records)} records from {len(source_files)} files in {end_time-start_time:.2f} seconds")
    
    # Get column names
    if all_records:
        print(f"\nColumns in the data: {', '.join(all_records[0].keys())}")
    
    # Display sample records
    print(f"\nShowing first {display_limit} records:")
    for i, record in enumerate(all_records[:display_limit]):
        print(f"\n=== Record {i+1} ===")
        for key, value in record.items():
            # Truncate long text fields
            if isinstance(value, str) and len(value) > 100:
                print(f"  {key}: {value[:100]}...")
            else:
                print(f"  {key}: {value}")
    
    # Display summary statistics
    if all_records:
        print("\n=== Summary Statistics ===")
        
        # Convert to DataFrame for easier analysis
        df = pd.DataFrame(all_records)
        
        # Count recommendations
        if 'recommended' in df.columns:
            recommended_count = df['recommended'].sum()
            not_recommended_count = len(df) - recommended_count
            print(f"Recommended: {recommended_count} ({recommended_count/len(df)*100:.1f}%)")
            print(f"Not Recommended: {not_recommended_count} ({not_recommended_count/len(df)*100:.1f}%)")
        
        # Show game distribution
        if 'app_id' in df.columns:
            top_games = df['app_id'].value_counts().head(5)
            print("\nTop 5 games by review count:")
            for game_id, count in top_games.items():
                print(f"  Game ID {game_id}: {count} reviews")
        
        # Language distribution if available
        if 'language' in df.columns:
            top_languages = df['language'].value_counts().head(3)
            print("\nTop 3 languages:")
            for language, count in top_languages.items():
                print(f"  {language}: {count} reviews ({count/len(df)*100:.1f}%)")
    
    print("\n=== Parquet Reader Test Completed ===")

if __name__ == "__main__":
    # Test with 100 records by default
    test_parquet_reading(num_records=120, display_limit=5) 