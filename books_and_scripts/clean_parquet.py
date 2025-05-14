from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, to_timestamp, count, when, isnan
from pyspark.sql.types import IntegerType, FloatType, BooleanType, TimestampType

def load_parquet_file(file_path):
    """
    Load the parquet file and return the DataFrame
    """
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("SteamReviewsAnalysis") \
        .config("spark.driver.memory", "8g") \
        .getOrCreate()
    
    # Load the dataset
    df = spark.read.parquet(file_path)
    
    print("Original Schema:")
    df.printSchema()
    
    return df, spark

def drop_columns(df, columns_to_drop):
    """
    Drop specified columns if they exist in the DataFrame
    """
    existing_columns = df.columns
    columns_to_drop_existing = [col for col in columns_to_drop if col in existing_columns]
    
    if columns_to_drop_existing:
        print(f"Dropping columns: {columns_to_drop_existing}")
        df = df.drop(*columns_to_drop_existing)
    else:
        print("None of the specified columns exist in the DataFrame")
    
    return df

def convert_data_types(df):
    """
    Convert columns to appropriate data types
    """
    df_processed = df \
        .withColumn("author_num_games_owned", col("author_num_games_owned").cast(IntegerType())) \
        .withColumn("author_num_reviews", col("author_num_reviews").cast(IntegerType())) \
        .withColumn("author_playtime_forever", col("author_playtime_forever").cast(IntegerType())) \
        .withColumn("author_playtime_at_review", col("author_playtime_at_review").cast(IntegerType())) \
        .withColumn("votes_up", col("votes_up").cast(IntegerType())) \
        .withColumn("votes_funny", col("votes_funny").cast(IntegerType())) \
        .withColumn("weighted_vote_score", col("weighted_vote_score").cast(FloatType())) \
        .withColumn("comment_count", col("comment_count").cast(IntegerType())) \
        .withColumn("steam_purchase", col("steam_purchase").cast(BooleanType())) \
        .withColumn("received_for_free", col("received_for_free").cast(BooleanType())) \
        .withColumn("written_during_early_access", col("written_during_early_access").cast(BooleanType()))
    
    # Convert Unix timestamps to proper datetime format
    df_processed = df_processed \
        .withColumn("author_last_played", to_timestamp(from_unixtime(col("author_last_played").cast("long")))) \
        .withColumn("timestamp_created", to_timestamp(from_unixtime(col("timestamp_created").cast("long")))) \
        .withColumn("timestamp_updated", to_timestamp(from_unixtime(col("timestamp_updated").cast("long"))))
    
    print("\nConverted Schema:")
    df_processed.printSchema()
    
    return df_processed

def check_null_values(df):
    """
    Check for null values in all columns and print the count
    """
    # Create expressions for counting nulls in each column
    null_count_expressions = []
    
    for c in df.columns:
        column_type = dict(df.dtypes)[c]
        
        # For numeric columns (int, double, float), check both null and NaN
        if column_type in ['int', 'bigint', 'double', 'float']:
            null_count_expressions.append(count(when(col(c).isNull() | isnan(c), c)).alias(c))
        # For string columns, check for nulls and empty strings
        elif column_type == 'string':
            null_count_expressions.append(count(when(col(c).isNull() | (col(c) == ""), c)).alias(c))
        # For timestamp columns, check for nulls
        elif column_type == 'timestamp':
            null_count_expressions.append(count(when(col(c).isNull(), c)).alias(c))
        # For other types, only check for null
        else:
            null_count_expressions.append(count(when(col(c).isNull(), c)).alias(c))
    
    # Apply the expressions to the DataFrame
    null_counts = df.select(null_count_expressions)
    
    return null_counts

def drop_rows_with_nulls(df):
    """
    Drop rows where at least one value is null or empty string (for string columns) 
    or NaN (for numeric columns)
    """
    # Get the data types of each column
    column_types = dict(df.dtypes)
    
    # Create a filter condition to keep rows where all columns are non-null/non-empty/non-NaN
    filter_condition = None
    
    for c in df.columns:
        column_type = column_types[c]
        
        # For numeric columns (int, double, float), check both null and NaN
        if column_type in ['int', 'bigint', 'double', 'float']:
            condition = ~(col(c).isNull() | isnan(col(c)))
        # For string columns, check for nulls and empty strings
        elif column_type == 'string':
            condition = ~(col(c).isNull() | (col(c) == ""))
        # For timestamp columns, check for nulls
        elif column_type == 'timestamp':
            condition = ~col(c).isNull()
        # For other types, only check for null
        else:
            condition = ~col(c).isNull()
        
        # Combine conditions with AND
        if filter_condition is None:
            filter_condition = condition
        else:
            filter_condition = filter_condition & condition
    
    # Count before dropping
    count_before = df.count()
    
    # Filter the dataframe
    df_cleaned = df.filter(filter_condition)
    
    # Count after dropping
    count_after = df_cleaned.count()
    
    print(f"Dropped {count_before - count_after} rows with null values")
    print(f"Rows before: {count_before}, Rows after: {count_after}")
    
    return df_cleaned

def main(file_path, output_path):
    """
    Main function to process the Steam Reviews dataset
    """
    # TASK 1: Load the parquet file
    df, spark = load_parquet_file(file_path)
    
    # TASK 2: Drop specified columns
    columns_to_drop = [
        "author_playtime_last_two_weeks", 
        "recommendationid",
        "appid",
        "voted_up",
        "hidden_in_steam_china",
        "steam_china_location"
    ]
    df = drop_columns(df, columns_to_drop)
    
    # TASK 3: Convert data types
    df = convert_data_types(df)
    
    # TASK 4: Drop rows with null values
    # First, check for null values
    null_counts = check_null_values(df)
    print("Null value counts before dropping rows:")
    null_counts.show(truncate=False)
    
    # Drop rows with null values
    df_cleaned = drop_rows_with_nulls(df)
    
    # Verify no null values remain
    null_counts_after = check_null_values(df_cleaned)
    print("Null value counts after dropping rows:")
    null_counts_after.show(truncate=False)
    
    # TASK 5: Save the cleaned DataFrame as a single parquet file
    print(f"Saving cleaned data to {output_path}")
    df_cleaned.coalesce(1).write \
        .option("compression", "snappy") \
        .mode("overwrite") \
        .parquet(output_path)
    
    print("Cleaned parquet file saved successfully.")
    
    # Stop Spark session
    spark.stop()
    
    return df_cleaned

if __name__ == "__main__":
    # Input and output file paths
    input_file_path = "D:/STUFF/Projects/BigData_Project/data/all_reviews/main_data.parquet"
    output_file_path = "D:/STUFF/Projects/BigData_Project/data/all_reviews/new_attempt"
    
    # Process the dataset
    clean_df = main(input_file_path, output_file_path)