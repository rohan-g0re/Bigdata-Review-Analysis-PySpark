"""
Test script for the Parquet reader module.
This verifies that our Parquet reader works correctly.
"""
import os
import argparse
import json
from typing import Dict, Any

from src.streaming.utils.logging_config import setup_logger
from src.streaming.producer.parquet_reader import ParquetReader
from src.streaming.config.settings import PARQUET_DIR, BATCH_SIZE

logger = setup_logger("test_parquet_reader", "test_parquet_reader.log")

def test_scan_directory(parquet_dir: str) -> bool:
    """
    Test scanning the directory for Parquet files
    
    Args:
        parquet_dir (str): Directory containing Parquet files
        
    Returns:
        bool: True if successful
    """
    try:
        logger.info(f"Testing directory scanning: {parquet_dir}")
        reader = ParquetReader(parquet_dir=parquet_dir)
        
        file_info = reader.get_file_info()
        print(f"Found {file_info['file_count']} Parquet files")
        print(f"Total size: {file_info['total_size_mb']:.2f} MB")
        
        return True
    except Exception as e:
        logger.error(f"Failed to scan directory: {str(e)}")
        return False

def test_schema(parquet_dir: str) -> bool:
    """
    Test reading the schema from Parquet files
    
    Args:
        parquet_dir (str): Directory containing Parquet files
        
    Returns:
        bool: True if successful
    """
    try:
        logger.info(f"Testing schema reading")
        reader = ParquetReader(parquet_dir=parquet_dir)
        
        schema = reader.read_schema()
        print(f"Schema has {len(schema.names)} columns:")
        for i, field in enumerate(schema):
            print(f"  {i+1}. {field.name}: {field.type}")
        
        return True
    except Exception as e:
        logger.error(f"Failed to read schema: {str(e)}")
        return False

def test_sample(parquet_dir: str, num_records: int = 2) -> bool:
    """
    Test reading a sample of records
    
    Args:
        parquet_dir (str): Directory containing Parquet files
        num_records (int): Number of records to read
        
    Returns:
        bool: True if successful
    """
    try:
        logger.info(f"Testing sample reading ({num_records} records)")
        reader = ParquetReader(parquet_dir=parquet_dir)
        
        sample = reader.read_sample(num_records=num_records)
        print(f"Read {len(sample)} sample records")
        
        # Print the first record in a formatted way
        if sample:
            print("\nSample record:")
            print(json.dumps(sample[0], indent=2))
        
        return True
    except Exception as e:
        logger.error(f"Failed to read sample: {str(e)}")
        return False

def test_batch_reading(parquet_dir: str, batch_size: int, max_batches: int = 2) -> bool:
    """
    Test reading files in batches
    
    Args:
        parquet_dir (str): Directory containing Parquet files
        batch_size (int): Batch size to use
        max_batches (int): Maximum number of batches to read
        
    Returns:
        bool: True if successful
    """
    try:
        logger.info(f"Testing batch reading (batch_size={batch_size}, max_batches={max_batches})")
        reader = ParquetReader(parquet_dir=parquet_dir, batch_size=batch_size)
        
        # Get the first file
        if not reader.files:
            logger.error("No Parquet files found")
            return False
        
        first_file = reader.files[0]
        print(f"Reading from file: {os.path.basename(first_file)}")
        
        # Read batches
        batch_count = 0
        total_records = 0
        
        for batch in reader.read_file_batches(first_file):
            batch_count += 1
            total_records += len(batch)
            
            print(f"Read batch {batch_count}: {len(batch)} records")
            
            if batch_count >= max_batches:
                break
        
        print(f"Total records read: {total_records}")
        
        return True
    except Exception as e:
        logger.error(f"Failed to read batches: {str(e)}")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Test the Parquet reader')
    parser.add_argument('--parquet-dir', default=PARQUET_DIR,
                        help='Directory containing Parquet files')
    parser.add_argument('--batch-size', type=int, default=BATCH_SIZE,
                        help='Batch size for reading')
    parser.add_argument('--test', choices=['scan', 'schema', 'sample', 'batch', 'all'], default='all',
                        help='Which test to run')
    parser.add_argument('--max-batches', type=int, default=2,
                        help='Maximum number of batches to read in batch test')
    parser.add_argument('--sample-size', type=int, default=2,
                        help='Number of records to read in sample test')
    
    args = parser.parse_args()
    
    if args.test in ['scan', 'all']:
        print("\n----- Testing Directory Scanning -----")
        success = test_scan_directory(args.parquet_dir)
        print("Directory scanning test:", "SUCCESS" if success else "FAILED")
    
    if args.test in ['schema', 'all']:
        print("\n----- Testing Schema Reading -----")
        success = test_schema(args.parquet_dir)
        print("Schema reading test:", "SUCCESS" if success else "FAILED")
    
    if args.test in ['sample', 'all']:
        print("\n----- Testing Sample Reading -----")
        success = test_sample(args.parquet_dir, args.sample_size)
        print("Sample reading test:", "SUCCESS" if success else "FAILED")
    
    if args.test in ['batch', 'all']:
        print("\n----- Testing Batch Reading -----")
        success = test_batch_reading(args.parquet_dir, args.batch_size, args.max_batches)
        print("Batch reading test:", "SUCCESS" if success else "FAILED") 