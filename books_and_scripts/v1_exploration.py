#!/usr/bin/env python
# coding: utf-8

# # v1

# In[6]:


from pyspark.sql import SparkSession

# Initialize SparkSession with appropriate memory configuration
spark = SparkSession.builder \
    .appName("Large Parquet File Analysis") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "4") \
    .config("spark.sql.files.maxPartitionBytes", "134217728") \
    .getOrCreate()


# In[42]:


# Path to your parquet file
file_path = "data/all_reviews/1m_sample.parquet"

# Read the parquet file
df = spark.read.parquet(file_path)

# Display the schema
df.printSchema()



# In[43]:


# Show a sample of the data (first 10 rows)
df.head(1)


# In[44]:


from pyspark.sql.functions import col, rand, when, isnan, length

def show_null_or_zero_samples(dataframe, column_name, sample_size=10):
    """
    Display random samples of records where the specified column is null or zero.
    
    Parameters:
    dataframe (DataFrame): The PySpark DataFrame to analyze
    column_name (str): The name of the column to check for null/zero values
    sample_size (int): Number of sample records to display (default: 10)
    """
    # Check if column exists in the dataframe
    if column_name not in dataframe.columns:
        print(f"Column '{column_name}' not found in the dataframe.")
        print(f"Available columns: {dataframe.columns}")
        return
    
    # Get the data type of the column
    column_type = dict(dataframe.dtypes)[column_name]
    
    # Create appropriate filter condition based on column type
    if column_type in ['int', 'bigint', 'double', 'float', 'decimal']:
        # For numeric columns, check for nulls, NaN, or zero
        filter_condition = (col(column_name).isNull() | 
                           (when(column_type in ['double', 'float'], isnan(col(column_name))).otherwise(False)) | 
                           (col(column_name) == 0))
    elif column_type in ['boolean']:
        # For boolean columns, check for nulls or False
        filter_condition = (col(column_name).isNull() | (col(column_name) == False))
    elif column_type in ['timestamp', 'date']:
        # For timestamp/date columns, check for nulls only
        filter_condition = col(column_name).isNull()
    elif column_type == 'string':
        # For string columns, check for nulls, empty strings, or whitespace-only strings
        filter_condition = (col(column_name).isNull() | 
                           (col(column_name) == "") | 
                           (length(trim(col(column_name))) == 0))
    else:
        # For other types, check for nulls only
        filter_condition = col(column_name).isNull()
    
    # Filter the dataframe and get random samples
    filtered_df = dataframe.filter(filter_condition)
    
    # Count the number of matching records
    count = filtered_df.count()
    
    if count == 0:
        print(f"No records found where '{column_name}' is null or zero.")
        return
    
    # Order by random to get random samples
    samples = filtered_df.orderBy(rand()).limit(sample_size)
    
    print(f"Found {count} records where '{column_name}' is null or zero.")
    print(f"Showing {min(sample_size, count)} random samples:")
    samples.show(sample_size, truncate=False)
    
    # Return the filtered dataframe in case further analysis is needed
    return filtered_df



# In[33]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, LongType, BooleanType, FloatType, TimestampType



# In[34]:


# Assuming df is your existing DataFrame
df_processed = df \
    .withColumn("appid", col("appid").cast(IntegerType())) \
    .withColumn("author_num_games_owned", col("author_num_games_owned").cast(IntegerType())) \
    .withColumn("author_num_reviews", col("author_num_reviews").cast(IntegerType())) \
    .withColumn("author_playtime_forever", col("author_playtime_forever").cast(IntegerType())) \
    .withColumn("author_playtime_last_two_weeks", col("author_playtime_last_two_weeks").cast(IntegerType())) \
    .withColumn("author_playtime_at_review", col("author_playtime_at_review").cast(IntegerType())) \
    .withColumn("author_last_played", col("author_last_played").cast(LongType())) \
    .withColumn("votes_up", col("votes_up").cast(IntegerType())) \
    .withColumn("votes_funny", col("votes_funny").cast(IntegerType())) \
    .withColumn("weighted_vote_score", col("weighted_vote_score").cast(FloatType())) \
    .withColumn("comment_count", col("comment_count").cast(IntegerType())) \
    .withColumn("voted_up", col("voted_up").cast(BooleanType())) \
    .withColumn("steam_purchase", col("steam_purchase").cast(BooleanType())) \
    .withColumn("received_for_free", col("received_for_free").cast(BooleanType())) \
    .withColumn("written_during_early_access", col("written_during_early_access").cast(BooleanType()))


    # .withColumn("timestamp_created", col("timestamp_created").cast(TimestampType())) \
    # .withColumn("timestamp_updated", col("timestamp_updated").cast(TimestampType()))


# In[35]:


df_processed.printSchema()


# In[ ]:


show_null_or_zero_samples(df_processed, "game")


# In[36]:


from pyspark.sql.functions import count, when, isnan, col

# Get the data types of each column
column_types = dict(df_processed.dtypes)

# Create a list to store expressions for counting nulls
null_count_expressions = []

for c in df_processed.columns:
    # For numeric columns (double/float), check both null and NaN
    if column_types[c] in ['integer', 'double', 'float']:
        null_count_expressions.append(count(when(col(c).isNull() | isnan(c), c)).alias(c))
    # For other types, only check for null
    else:
        null_count_expressions.append(count(when(col(c).isNull(), c)).alias(c))

# Apply the expressions to the DataFrame
null_counts = df_processed.select(null_count_expressions)


# In[37]:


null_counts.head(50)


# In[38]:





# In[40]:


# Example usage:
show_null_or_zero_samples(df_processed, "game")


# In[ ]:





# In[ ]:





# # v2

# In[ ]:





# In[1]:


from pyspark.sql import SparkSession

# Initialize SparkSession with appropriate memory configuration
spark = SparkSession.builder \
    .appName("Large Parquet File Analysis") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "4") \
    .config("spark.sql.files.maxPartitionBytes", "134217728") \
    .getOrCreate()


# In[2]:


import pandas as pd
import numpy as np
import random
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, isnan, count, rand, trim, length
from pyspark.sql.types import IntegerType, LongType, BooleanType, FloatType

# 1. Load data and print schema
def load_data(file_path):
    """
    Load the parquet file and print its schema
    """    
    # Load the dataset
    df = spark.read.parquet(file_path)
    
    # Print the schema
    print("Original Schema:")
    df.printSchema()
    
    return df

file_path = "data/all_reviews/1m_sample.parquet"

# Read the parquet file
df = load_data(file_path)



# In[37]:


# 2. Check for null values
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
        # For other types, only check for null
        else:
            null_count_expressions.append(count(when(col(c).isNull(), c)).alias(c))
    
    # Apply the expressions to the DataFrame
    null_counts = df.select(null_count_expressions)
    
    # print("Null value counts for each column:")
    # null_counts.show(truncate=False)
    
    return null_counts


null_counts = check_null_values(df)


# In[38]:


null_counts.head(23)


# In[ ]:





# In[40]:


# 3. Convert data types
from pyspark.sql.functions import col, from_unixtime, to_timestamp
from pyspark.sql.types import IntegerType, FloatType, BooleanType, TimestampType

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



df_dtypes_changed = convert_data_types(df)


# In[41]:


null_counts = check_null_values(df_dtypes_changed)
null_counts.head(50)


# In[ ]:





# In[42]:


df_dtypes_changed.show(100)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[7]:


from pyspark.sql.functions import col, countDistinct, desc, isnan, when, count

def analyze_string_columns_for_conversion(df, columns):
    """
    Analyze string columns before conversion to identify potential issues
    
    Parameters:
    df (DataFrame): The PySpark DataFrame to analyze
    columns (list): List of column names to analyze
    """
    for column in columns:
        print(f"\n{'='*80}")
        print(f"ANALYSIS FOR COLUMN: {column}")
        print(f"{'='*80}")
        
        # 1. Count total unique values
        unique_count = df.select(countDistinct(col(column))).collect()[0][0]
        print(f"Total unique values: {unique_count}")
        
        # 2. Count null or empty strings
        null_empty_count = df.filter((col(column).isNull()) | (col(column) == "") | (col(column).rlike('^\\s*$'))).count()
        print(f"Count of null or empty strings: {null_empty_count}")
        
        # 3. Show sample unique values (up to 20)
        unique_values = df.select(column).distinct().limit(20).rdd.flatMap(lambda x: x).collect()
        print(f"Sample unique values (up to 20): {unique_values}")
        
        # 4. Show top 10 most frequent values
        print("Top 10 most frequent values:")
        df.groupBy(column).count().orderBy(desc("count")).limit(20).show(truncate=False)
        
        # 5. Check for non-convertible values based on target data type
        if column in ["voted_up", "steam_purchase", "received_for_free", "written_during_early_access"]:
            # Boolean columns - check for values other than 0, 1, true, false, etc.
            non_bool_values = df.filter(
                ~col(column).isNull() & 
                ~col(column).rlike("^(0|1|true|false|t|f|yes|no|y|n)$")
            )
            non_bool_count = non_bool_values.count()
            print(f"Values that may not convert to boolean: {non_bool_count}")
            if non_bool_count > 0:
                print("Sample problematic values:")
                non_bool_values.select(column).distinct().limit(10).show(truncate=False)
                
        elif column in ["votes_up", "votes_funny", "comment_count"]:
            # Integer columns - check for non-numeric values
            non_int_values = df.filter(
                ~col(column).isNull() & 
                ~col(column).rlike("^-?\\d+$")
            )
            non_int_count = non_int_values.count()
            print(f"Values that may not convert to integer: {non_int_count}")
            if non_int_count > 0:
                print("Sample problematic values:")
                non_int_values.select(column).distinct().limit(10).show(truncate=False)
                
        elif column == "weighted_vote_score":
            # Float column - check for non-numeric values
            non_float_values = df.filter(
                ~col(column).isNull() & 
                ~col(column).rlike("^-?\\d+(\\.\\d*)?$")
            )
            non_float_count = non_float_values.count()
            print(f"Values that may not convert to float: {non_float_count}")
            if non_float_count > 0:
                print("Sample problematic values:")
                non_float_values.select(column).distinct().limit(10).show(truncate=False)
        
        # 6. Check for leading/trailing whitespace
        whitespace_values = df.filter(
            ~col(column).isNull() & 
            ((col(column) != trim(col(column))))
        )
        whitespace_count = whitespace_values.count()
        print(f"Values with leading/trailing whitespace: {whitespace_count}")
        
        # 7. Check for special characters that might cause issues
        special_char_values = df.filter(
            ~col(column).isNull() & 
            col(column).rlike("[^a-zA-Z0-9\\s\\.-]")
        )
        special_char_count = special_char_values.count()
        print(f"Values with special characters: {special_char_count}")



# In[8]:


# 5. Drop function
def drop_null_records(df, column_name):
    """
    Drop records where the specified column has null values
    """
    # Check if column exists
    if column_name not in df.columns:
        print(f"Column '{column_name}' not found in the dataframe.")
        return df
    
    # Count before dropping
    count_before = df.count()
    
    # Drop null records for the specified column
    column_type = dict(df.dtypes)[column_name]
    
    if column_type in ['double', 'float']:
        df_cleaned = df.filter(~(col(column_name).isNull() | isnan(col(column_name))))
    elif column_type == 'string':
        df_cleaned = df.filter(~(col(column_name).isNull() | (col(column_name) == "")))
    else:
        df_cleaned = df.filter(col(column_name).isNotNull())
    
    # Count after dropping
    count_after = df_cleaned.count()
    
    print(f"Dropped {count_before - count_after} records where '{column_name}' is null.")
    print(f"Records before: {count_before}, Records after: {count_after}")
    
    return df_cleaned



# In[ ]:





# In[ ]:





# In[ ]:





# In[45]:


null_counts = check_null_values(df_dtypes_changed)
null_counts.head(50)


# ### Here we do ANALYSIS

# In[43]:


columns_to_analyze = [
    "game"
]
analyze_string_columns_for_conversion(df, columns_to_analyze)


# In[ ]:





# In[ ]:





# In[ ]:





# In[44]:


columns_to_analyze = [
    "game"
]
analyze_string_columns_for_conversion(df_dtypes_changed, columns_to_analyze)


# In[ ]:





# In[ ]:





# In[ ]:





# In[142]:


# column_to_check = "game"  # Change this to any column you want to check
# print(f"\nShowing null samples for column: {column_to_check}")
# null_samples = show_null_samples(df, column_to_check)


# In[ ]:





# In[ ]:





# ### HERE WE DROP THE NULL VALUES FROM A COLUMN

# In[92]:





# In[23]:


print("\n--- STEP 5: Dropping null records ---")
column_to_drop = "votes_funny"  # Change this to the column you want to use for dropping
df_cleaned = drop_null_records(df_cleaned, column_to_drop)


# In[24]:


null_counts = check_null_values(df_cleaned)
null_counts.head(50)


# In[36]:


columns_to_analyze = [
    "timestamp_updated"
]

analyze_string_columns_for_conversion(df_cleaned, columns_to_analyze)


# ## TESTING DRIVER CODE

# In[46]:


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

def main(file_path):
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
    
    # Return the cleaned DataFrame
    return df_cleaned

if __name__ == "__main__":
    # Replace with your actual file path
    file_path = "data/all_reviews/1m_sample.parquet"
    
    # Process the dataset
    clean_df = main(file_path)
    



# In[47]:


null_counts = check_null_values(clean_df)
null_counts.head(50)


# In[48]:


column_to_drop = "game"  # Change this to the column you want to use for dropping
clean_df = drop_null_records(clean_df, column_to_drop)


# In[50]:


columns_to_analyze = [
    "timestamp_created"
]

analyze_string_columns_for_conversion(clean_df, columns_to_analyze)


# In[ ]:




