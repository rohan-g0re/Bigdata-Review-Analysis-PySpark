from pyspark.sql import SparkSession

def get_spark_session(app_name="SteamReviewsAnalysis", memory="8g"):
    """
    Create and return a Spark session with the specified configuration.
    
    Parameters:
    - app_name: Name of the Spark application
    - memory: Amount of driver memory to allocate
    
    Returns:
    - SparkSession object
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[4]") \
        .config("spark.driver.memory", memory) \
        .getOrCreate()
    
    return spark 