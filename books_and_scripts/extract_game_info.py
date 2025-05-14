from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_set
import os
import sys
import time

def main():
    """
    Main function to:
    1. Create a PySpark session and load parquet files
    2. Extract unique game names
    3. Extract unique languages in reviews
    4. Write all the names in a text file
    5. Write all unique languages to a text file
    """
    start_time = time.time()
    
    try:
        # Initialize SparkSession
        print("Initializing Spark session...")
        spark = SparkSession.builder \
            .appName("GameInfoExtractor") \
            .config("spark.driver.memory", "8g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.sql.shuffle.partitions", "10") \
            .getOrCreate()
        
        # Path to cleaned parquet files
        parquet_path = "data/all_reviews/cleaned_reviews"
        
        print(f"Loading parquet files from '{parquet_path}'...")
        # Load all parquet files in the directory
        df = spark.read.parquet(parquet_path)
        
        # Check if the DataFrame is empty
        row_count = df.count()
        if row_count == 0:
            print("Warning: The DataFrame is empty. Please check the path to parquet files.")
            return
        
        print("Data loaded successfully!")
        print(f"Number of records: {row_count}")
        print(f"Columns in the dataset: {df.columns}")
        
        # Identify the column names for game names and languages
        columns = df.columns
        
        # Try to find the most likely column for game names
        game_name_column = None
        possible_game_columns = ['app_name', 'game_name', 'title', 'name', 'game']
        for col_name in possible_game_columns:
            if col_name in columns:
                game_name_column = col_name
                break
        
        if not game_name_column:
            print("Could not find a column containing game names.")
            print("Available columns:", columns)
            return
        
        # Try to find the most likely column for languages
        language_column = None
        possible_language_columns = ['language', 'review_language', 'user_language']
        for col_name in possible_language_columns:
            if col_name in columns:
                language_column = col_name
                break
        
        if not language_column:
            print("Could not find a column containing languages.")
            print("Available columns:", columns)
            print("Continuing without extracting languages...")
        
        # Fetch ALL unique names of games
        print(f"Extracting unique game names from column '{game_name_column}'...")
        unique_game_names = df.select(collect_set(game_name_column)).collect()[0][0]
        print(f"Found {len(unique_game_names)} unique game names")
        
        # Fetch ALL unique languages in reviews (if language column was found)
        unique_languages = []
        if language_column:
            print(f"Extracting unique languages from column '{language_column}'...")
            unique_languages = df.select(collect_set(language_column)).collect()[0][0]
            print(f"Found {len(unique_languages)} unique languages")
            print("Languages:", sorted(unique_languages))
        
        # Write all unique game names to a text file
        output_file = "unique_game_names.txt"
        print(f"Writing unique game names to '{output_file}'...")
        
        with open(output_file, 'w', encoding='utf-8') as f:
            for game_name in sorted(unique_game_names):
                if game_name:  # Skip None values
                    f.write(f"{game_name}\n")
        
        print(f"Unique game names written to '{output_file}'")
        
        # Write all unique languages to a text file
        if unique_languages:
            lang_output_file = "unique_languages.txt"
            print(f"Writing unique languages to '{lang_output_file}'...")
            
            with open(lang_output_file, 'w', encoding='utf-8') as f:
                for language in sorted(unique_languages):
                    if language:  # Skip None values
                        f.write(f"{language}\n")
            
            print(f"Unique languages written to '{lang_output_file}'")
        
        # Stop Spark session
        spark.stop()
        
        # Calculate and print execution time
        execution_time = time.time() - start_time
        print(f"Script executed in {execution_time:.2f} seconds")
        
    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main() 