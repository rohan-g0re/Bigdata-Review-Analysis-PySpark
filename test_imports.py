import sys
import os

print("Python path:", sys.path)
print("\nCurrent directory:", os.getcwd())
print("\nListing directories:")
for root, dirs, files in os.walk("src"):
    for d in dirs:
        print(f"Directory: {os.path.join(root, d)}")
    
print("\nTrying imports:")
try:
    from src.streaming import utils
    print("- Successfully imported src.streaming.utils")
except Exception as e:
    print(f"- Error importing src.streaming.utils: {e}")

try:
    from src.streaming.utils import logging_config
    print("- Successfully imported src.streaming.utils.logging_config")
except Exception as e:
    print(f"- Error importing src.streaming.utils.logging_config: {e}")

try:
    from src.streaming.config import settings
    print("- Successfully imported src.streaming.config.settings")
except Exception as e:
    print(f"- Error importing src.streaming.config.settings: {e}")

try:
    from src.streaming.utils import kafka_utils
    print("- Successfully imported src.streaming.utils.kafka_utils")
except Exception as e:
    print(f"- Error importing src.streaming.utils.kafka_utils: {e}")

try:
    from src.streaming.utils import create_topic
    print("- Successfully imported src.streaming.utils.create_topic")
except Exception as e:
    print(f"- Error importing src.streaming.utils.create_topic: {e}")

try:
    from src.streaming.utils import test_kafka
    print("- Successfully imported src.streaming.utils.test_kafka")
except Exception as e:
    print(f"- Error importing src.streaming.utils.test_kafka: {e}") 