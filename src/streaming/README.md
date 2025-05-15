# Steam Game Reviews Streaming

This module implements real-time streaming capabilities for the Steam Game Reviews Analysis project. It uses Kafka for streaming data from Parquet files and PySpark Structured Streaming for processing.

## Setup and Installation

### Prerequisites

- Docker and Docker Compose for running Kafka
- Python 3.8+ with dependencies from requirements.txt
- Apache Spark (included in PySpark)

### Starting the Kafka Environment

1. From the project root, start the Kafka environment:

```bash
docker-compose up -d
```

This will start ZooKeeper, Kafka, and the Kafka UI accessible at http://localhost:8080.

2. Create the Kafka topic:

```bash
python -m src.streaming.utils.create_topic
```

### Testing the Kafka Setup

To verify that Kafka is working correctly:

```bash
python -m src.streaming.utils.test_kafka
```

This will send some test messages to the Kafka topic and then consume them to verify the setup.

## Project Structure

- `src/streaming/producer`: Kafka producer for streaming Parquet data
- `src/streaming/consumer`: Spark streaming consumer for processing data
- `src/streaming/utils`: Utility functions for Kafka and logging
- `src/streaming/config`: Configuration settings

## Configuration

Configuration is handled through environment variables or a `.env` file. See `src/streaming/config/settings.py` for available options. Key settings include:

- `KAFKA_BOOTSTRAP_SERVERS`: Kafka broker addresses (default: "localhost:9092")
- `KAFKA_TOPIC`: Topic name for Steam reviews (default: "steam_reviews_stream")
- `BATCH_SIZE`: Number of rows per Kafka message (default: 500)
- `THROTTLE_RATE`: Seconds between batches (default: 0.2)

## Development Guide

### Adding New Streaming Components

1. Create new module in appropriate directory
2. Update imports and references
3. Register in dashboard if needed
4. Add tests

### Running Tests

Basic Kafka tests:

```bash
python -m src.streaming.utils.test_kafka
```

## Troubleshooting

Common issues and their solutions:

1. **Cannot connect to Kafka**: Ensure Docker containers are running (`docker ps`)
2. **Missing logs directory**: The application will create it automatically
3. **ImportError**: Ensure paths are set correctly and you're running from the project root 