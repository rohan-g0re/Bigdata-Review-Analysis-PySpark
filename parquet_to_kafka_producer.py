from kafka import KafkaProducer
import json
import time
from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder.appName("ParquetToKafka").getOrCreate()

# Load multiple Parquet files (streamed)
df = spark.read.parquet("data/cleaned_reviews_parquet/*.parquet")

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Safely iterate over big data without collecting
for row in df.toLocalIterator():
    json_record = row.asDict()
    producer.send("steam-reviews", value=json_record)
    print("Sent to Kafka:", json_record)
    time.sleep(0.05)  # adjust delay as needed

producer.flush()

