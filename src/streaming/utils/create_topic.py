import argparse
from src.streaming.utils.logging_config import setup_logger
from src.streaming.utils.kafka_utils import create_kafka_topic
from src.streaming.config.settings import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, USE_MOCK_KAFKA

logger = setup_logger("create_topic", "create_topic.log")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Create Kafka topic for Steam reviews')
    parser.add_argument('--bootstrap-servers', default=KAFKA_BOOTSTRAP_SERVERS,
                        help='Kafka bootstrap servers')
    parser.add_argument('--topic', default=KAFKA_TOPIC,
                        help='Topic name')
    parser.add_argument('--partitions', type=int, default=3,
                        help='Number of partitions')
    parser.add_argument('--replication-factor', type=int, default=1,
                        help='Replication factor')
    
    args = parser.parse_args()
    
    logger.info(f"Using {'Mock' if USE_MOCK_KAFKA else 'Real'} Kafka implementation")
    
    result = create_kafka_topic(
        args.bootstrap_servers,
        args.topic,
        args.partitions,
        args.replication_factor
    )
    
    if result:
        print(f"Topic '{args.topic}' created or already exists")
    else:
        print(f"Failed to create topic '{args.topic}'")
        exit(1) 