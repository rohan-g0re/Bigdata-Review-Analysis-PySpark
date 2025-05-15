import os
from dotenv import load_dotenv

# Load environment variables from .env file (if it exists)
load_dotenv()

# Mock Kafka setting (set to True to use in-memory mock instead of real Kafka)
USE_MOCK_KAFKA = os.getenv("USE_MOCK_KAFKA", "True").lower() in ("true", "1", "yes")

# Kafka settings
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "steam_reviews_stream")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "steam_reviews_group")
KAFKA_AUTO_OFFSET_RESET = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")

# Producer settings
PARQUET_DIR = os.getenv("PARQUET_DIR", "data/all_reviews/cleaned_reviews")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))  # Number of rows per Kafka message
THROTTLE_RATE = float(os.getenv("THROTTLE_RATE", "0.2"))  # Seconds between batches

# Consumer settings
SPARK_CHECKPOINT_LOCATION = os.getenv("SPARK_CHECKPOINT_LOCATION", "spark_temp/checkpoints")
SPARK_PROCESSING_TIME = os.getenv("SPARK_PROCESSING_TIME", "10 seconds")

# Output settings for streaming results
STREAM_RESULTS_DIR = os.getenv("STREAM_RESULTS_DIR", "data/stream_results")

# Dashboard refresh settings
DASHBOARD_REFRESH_INTERVAL = int(os.getenv("DASHBOARD_REFRESH_INTERVAL", "5")) 