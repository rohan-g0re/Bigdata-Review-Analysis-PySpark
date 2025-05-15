"""
Parquet Reader module for reading and batching Steam review data from Parquet files
"""
import os
import time
import glob
from typing import List, Dict, Any, Generator, Optional, Tuple
import json
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

from src.streaming.utils.logging_config import setup_logger
from src.streaming.config.settings import PARQUET_DIR, BATCH_SIZE

logger = setup_logger("parquet_reader", "parquet_reader.log")

class ParquetReader:
    """
    Class for reading and batching Steam review data from Parquet files
    """
    
    def __init__(self, parquet_dir: str = PARQUET_DIR, batch_size: int = BATCH_SIZE):
        """
        Initialize the ParquetReader
        
        Args:
            parquet_dir (str): Directory containing Parquet files
            batch_size (int): Number of rows to include in each batch
        """
        self.parquet_dir = parquet_dir
        self.batch_size = batch_size
        self.files = []
        self.total_size_bytes = 0
        self.file_stats = {}
        
        # Scan the directory for Parquet files
        self._scan_directory()
        
    def _scan_directory(self) -> None:
        """
        Scan the directory for Parquet files and collect metadata
        """
        logger.info(f"Scanning directory for Parquet files: {self.parquet_dir}")
        
        if not os.path.exists(self.parquet_dir):
            logger.error(f"Directory does not exist: {self.parquet_dir}")
            raise FileNotFoundError(f"Directory does not exist: {self.parquet_dir}")
        
        # Find all Parquet files in the directory
        parquet_files = glob.glob(os.path.join(self.parquet_dir, "*.parquet"))
        
        if not parquet_files:
            logger.error(f"No Parquet files found in directory: {self.parquet_dir}")
            raise FileNotFoundError(f"No Parquet files found in directory: {self.parquet_dir}")
        
        # Collect file statistics
        for file_path in parquet_files:
            file_size = os.path.getsize(file_path)
            file_name = os.path.basename(file_path)
            
            self.files.append(file_path)
            self.file_stats[file_path] = {
                "size_bytes": file_size,
                "name": file_name
            }
            
            self.total_size_bytes += file_size
        
        # Sort files by name for deterministic processing
        self.files.sort()
        
        logger.info(f"Found {len(self.files)} Parquet files, total size: {self.total_size_bytes / (1024**2):.2f} MB")
    
    def get_file_info(self) -> Dict[str, Any]:
        """
        Get information about the Parquet files in the directory
        
        Returns:
            Dict: Information about the Parquet files
        """
        return {
            "directory": self.parquet_dir,
            "file_count": len(self.files),
            "total_size_mb": self.total_size_bytes / (1024**2),
            "files": self.file_stats
        }
    
    def read_schema(self) -> pa.Schema:
        """
        Read the schema from the first Parquet file
        
        Returns:
            pyarrow.Schema: Schema of the Parquet file
        """
        if not self.files:
            logger.error("No Parquet files available")
            raise ValueError("No Parquet files available")
        
        try:
            schema = pq.read_schema(self.files[0])
            logger.info(f"Read schema with {len(schema.names)} columns")
            return schema
        except Exception as e:
            logger.error(f"Error reading schema: {str(e)}")
            raise
    
    def _convert_arrow_to_dict(self, batch: pa.RecordBatch) -> List[Dict[str, Any]]:
        """
        Convert an Arrow RecordBatch to a list of dictionaries without using pandas
        
        Args:
            batch (pa.RecordBatch): Arrow RecordBatch to convert
            
        Returns:
            List[Dict[str, Any]]: List of dictionaries
        """
        records = []
        # Get number of rows
        num_rows = batch.num_rows
        
        # Process each row
        for i in range(num_rows):
            record = {}
            
            # Process each column
            for j, col_name in enumerate(batch.schema.names):
                # Get the value
                value = batch.column(j)[i].as_py()
                
                # Convert datetime objects to string
                if hasattr(value, 'strftime'):  # Check if it's a datetime-like object
                    value = value.strftime('%Y-%m-%d %H:%M:%S')
                    
                record[col_name] = value
                
            records.append(record)
            
        return records
    
    def read_file_batches(self, file_path: str) -> Generator[List[Dict[str, Any]], None, None]:
        """
        Read a single Parquet file in batches
        
        Args:
            file_path (str): Path to the Parquet file
            
        Yields:
            List[Dict[str, Any]]: Batch of records as dictionaries
        """
        try:
            logger.info(f"Reading file in batches: {os.path.basename(file_path)}")
            
            # Create a ParquetFile object
            parquet_file = pq.ParquetFile(file_path)
            
            # Read and yield batches
            for batch in parquet_file.iter_batches(batch_size=self.batch_size):
                # Convert Arrow RecordBatch to list of dictionaries directly
                records = self._convert_arrow_to_dict(batch)
                
                yield records
                
        except Exception as e:
            logger.error(f"Error reading file {file_path}: {str(e)}")
            raise
    
    def read_all_batches(self, show_progress: bool = True) -> Generator[Tuple[List[Dict[str, Any]], str], None, None]:
        """
        Read all Parquet files in the directory in batches
        
        Args:
            show_progress (bool): Whether to show a progress bar
            
        Yields:
            Tuple[List[Dict[str, Any]], str]: (Batch of records as dictionaries, source file path)
        """
        files_to_process = self.files
        
        if show_progress:
            files_to_process = tqdm(files_to_process, desc="Processing Parquet files")
        
        for file_path in files_to_process:
            try:
                for batch in self.read_file_batches(file_path):
                    yield batch, file_path
            except Exception as e:
                logger.error(f"Error processing file {file_path}: {str(e)}")
                logger.info(f"Skipping file and continuing with next file")
                continue

    def read_sample(self, num_records: int = 10, file_index: int = 0) -> List[Dict[str, Any]]:
        """
        Read a sample of records from a Parquet file
        
        Args:
            num_records (int): Number of records to read
            file_index (int): Index of the file to read from
            
        Returns:
            List[Dict[str, Any]]: Sample records as dictionaries
        """
        if not self.files:
            logger.error("No Parquet files available")
            raise ValueError("No Parquet files available")
        
        if file_index >= len(self.files):
            logger.error(f"File index {file_index} out of range (0-{len(self.files)-1})")
            raise IndexError(f"File index {file_index} out of range (0-{len(self.files)-1})")
        
        try:
            file_path = self.files[file_index]
            logger.info(f"Reading sample of {num_records} records from {os.path.basename(file_path)}")
            
            # Create a ParquetFile object
            parquet_file = pq.ParquetFile(file_path)
            
            # Read only the first batch with num_records
            batch = next(parquet_file.iter_batches(batch_size=num_records))
            
            # Convert to list of dictionaries
            records = self._convert_arrow_to_dict(batch)
            
            # Truncate to the requested number of records
            if len(records) > num_records:
                records = records[:num_records]
            
            return records
            
        except Exception as e:
            logger.error(f"Error reading sample: {str(e)}")
            raise


if __name__ == "__main__":
    # Example usage
    reader = ParquetReader()
    print(f"Found {len(reader.files)} Parquet files")
    
    # Print the schema
    schema = reader.read_schema()
    print("\nSchema:")
    print(schema)
    
    # Read and print a sample
    sample = reader.read_sample(5)
    print("\nSample of 5 records:")
    for i, record in enumerate(sample):
        print(f"\nRecord {i+1}:")
        for key, value in record.items():
            print(f"  {key}: {value}") 