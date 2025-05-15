# Steam Reviews Kafka Consumer

This module implements the Kafka consumer for the Steam Game Reviews streaming pipeline. It processes real-time streaming data from Kafka and performs analytics on the data.

## Features

- Real-time consumption of review data from Kafka
- Streaming analytics including:
  - Top games by review count
  - Sentiment analysis by game
  - Time distribution of reviews
  - Recent reviews tracking
- Dashboard integration via CSV output files
- Support for both real Kafka and mock Kafka environments
- Graceful shutdown and error handling

## Usage

### Running the Consumer

To start the consumer, run:

```bash
# From the project root directory
python -m src.streaming.consumer.kafka_consumer
```

### Testing

To test the consumer with mock data:

```bash
# Test with mock data
python -m src.streaming.consumer.test_consumer --mode=mock --samples=50

# Test end-to-end flow (producer to consumer)
python -m src.streaming.consumer.test_consumer --mode=e2e --samples=50
```

### Configuration

The consumer is configured through environment variables or the `.env` file. Key settings include:

- `KAFKA_BOOTSTRAP_SERVERS`: Kafka broker addresses (default: "localhost:9092")
- `KAFKA_TOPIC`: Topic name for Steam reviews (default: "steam_reviews_stream")
- `KAFKA_CONSUMER_GROUP`: Consumer group ID (default: "steam_reviews_group")
- `STREAM_RESULTS_DIR`: Directory to save analytics results (default: "data/stream_results")
- `USE_MOCK_KAFKA`: Whether to use mock Kafka (default: True)

## Integration with Dashboard

The consumer saves analytics results to CSV files in the `STREAM_RESULTS_DIR` directory. These files are automatically loaded by the Streamlit dashboard's Streaming Analytics tab.

## Architecture

The consumer consists of these key components:

1. **StreamConsumer**: Main class that manages Kafka consumption and processing
2. **StreamAnalytics**: Performs real-time analytics on the consumed data
3. **StreamingStats**: Tracks consumption statistics and performance metrics

## Troubleshooting

- If the consumer fails to connect to Kafka, verify that the Kafka service is running
- If results don't appear in the dashboard, check that the `STREAM_RESULTS_DIR` exists and is writable
- For testing without Kafka, ensure `USE_MOCK_KAFKA` is set to True 