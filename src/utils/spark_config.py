from pyspark.sql import SparkSession
import logging

def configure_spark():
    """Configure Spark session with appropriate settings."""
    # Set logging level
    logging.getLogger('py4j').setLevel(logging.ERROR)
    
    # Create Spark session with configurations
    spark = (SparkSession.builder
            .appName("SteamReviewsAnalysis")
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "4g")
            .config("spark.sql.shuffle.partitions", "200")
            .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:log4j.properties")
            .getOrCreate())
    
    # Set log level
    spark.sparkContext.setLogLevel("ERROR")
    
    return spark 