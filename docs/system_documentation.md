# Steam Game Reviews Analysis System Documentation

## System Overview

The Steam Game Reviews Analysis System is a real-time data processing pipeline built to analyze Steam game reviews. It consists of several integrated components that work together to stream, process, analyze, and visualize review data.

### Architecture Diagram

```
+-------------------+     +----------------+     +----------------+     +----------------+
| Source Data       |     | Producer       |     | Kafka          |     | Consumer       |
| (Parquet Files)   |---->| (Reads data &  |---->| (Message Queue)|---->| (Processes &   |
+-------------------+     | sends to Kafka)|     |                |     | analyzes data) |
                          +----------------+     +----------------+     +----------------+
                                                                              |
                                                                              v
                          +----------------+     +----------------+     +----------------+
                          | Monitoring     |<----| Metrics        |<----| Analytics      |
                          | Dashboard      |     | Collection     |     | Results        |
                          +----------------+     +----------------+     +----------------+
                                |                                              |
                                v                                              v
                          +----------------+                            +----------------+
                          | System Health  |                            | Streamlit      |
                          | Monitoring     |                            | Dashboard      |
                          +----------------+                            +----------------+
```

## Components

### 1. Producer

The Producer component reads Steam review data from Parquet files and sends it to a Kafka topic for real-time processing. It operates in batched mode for optimal performance.

**Key features:**
- Memory-efficient Parquet reading using PyArrow
- Configurable batch sizes and throttling 
- Detailed statistics tracking
- Threading support for parallel operations
- Metrics collection for performance monitoring

### 2. Message Queue (Kafka)

Apache Kafka serves as the message queue that decouples the producer from the consumer, enabling resilient and scalable stream processing.

**Key features:**
- Topics for message organization
- Partitioning for parallel processing
- Message durability
- Scalable architecture
- Mock implementation for development without Docker

### 3. Consumer

The Consumer component receives messages from Kafka, processes them, and performs real-time analytics on the streaming review data.

**Key features:**
- Configurable processing logic
- Thread-safe implementation
- Error handling and recovery
- Metrics reporting
- Periodic result saving

### 4. Analytics Engine

The Analytics Engine calculates real-time statistics and trends from the streaming review data.

**Key features:**
- Recent reviews tracking
- Sentiment analysis
- Game popularity metrics
- Time-based trend analysis
- Historical data comparison

### 5. Dashboard

The Streamlit dashboard visualizes both batch and streaming analytics in an interactive web interface.

**Key features:**
- Real-time data updates
- Interactive filters and controls
- Data mode toggle (batch vs streaming)
- Auto-refresh functionality
- Data freshness indicators

### 6. Monitoring System

The Monitoring System tracks the health and performance of all components in real time.

**Key features:**
- Component status monitoring
- Performance metrics collection
- Error tracking and alerting
- Resource usage monitoring
- End-to-end latency measurement

## Configuration

All system configuration is handled through a combination of:
- Environment variables
- Command-line arguments
- Configuration files

### Environment Variables

Key environment variables include:

| Variable | Description | Default |
|----------|-------------|---------|
| KAFKA_BOOTSTRAP_SERVERS | Kafka connection string | localhost:9092 |
| KAFKA_TOPIC | Topic for streaming reviews | steam_reviews |
| PARQUET_DIR | Directory with source Parquet files | data/all_reviews/cleaned_reviews |
| STREAM_RESULTS_DIR | Directory for streaming results | data/stream_results |
| BATCH_SIZE | Number of records per batch | 1000 |
| THROTTLE_RATE | Seconds between sending batches | 0.1 |
| USE_MOCK_KAFKA | Use mock implementation if true | false |

### Command Line Arguments

The system can be run with various command-line arguments to customize behavior. Examples:

```
# Run all components with default settings
python -m src.streaming.run_system

# Run only the producer with custom settings
python -m src.streaming.run_system --component producer --max-records 1000 --throttle-rate 0.2

# Run consumer and dashboard
python -m src.streaming.run_system --component all --max-messages 2000 --dashboard --monitor
```

## Performance Optimization

The system includes built-in performance optimization tools that analyze metrics and suggest configuration improvements:

- Batch size optimization
- Throttle rate tuning
- Memory usage optimization
- Thread configuration

Optimization techniques are automatically applied based on workload characteristics:
- For high-volume workloads, the system increases batch sizes and reduces throttling
- For complex analytics, the system prioritizes smaller batches to maintain latency targets
- For resource-constrained environments, memory usage is carefully managed

## Monitoring & Alerting

### System Metrics

The following metrics are tracked in real-time:

**Producer:**
- Records sent per second
- Batches sent
- Files processed
- Bytes transferred

**Consumer:**
- Records processed per second
- Games analyzed
- Processing time per message
- Error rate

**End-to-End:**
- Message latency
- Queue size
- Consumer lag

**System Resources:**
- CPU usage
- Memory consumption

### Alerting

The monitoring system provides alerts for:
- Component failures
- High latency
- Growing queue size
- High error rate
- Resource depletion

## Error Handling & Recovery

The system implements comprehensive error handling:

1. **Component-Level Recovery**
   - Each component can fail independently without affecting others
   - Automatic retry mechanisms for transient errors

2. **Message-Level Resilience**
   - Failed message processing doesn't halt the entire pipeline
   - Error tracking and reporting

3. **Graceful Degradation**
   - System continues operating with partial functionality when components fail
   - Critical paths are prioritized

4. **Restart Capabilities**
   - Components can be restarted independently
   - Consumer remembers processing position via Kafka offsets

## Running the System

### Prerequisites

- Python 3.8+
- Apache Kafka (or set USE_MOCK_KAFKA=true for development)
- Parquet files with Steam review data

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd steam-reviews-analysis

# Install dependencies
pip install -r requirements.txt
```

### Running Components

**1. Full System:**
```bash
python -m src.streaming.run_system --component all --dashboard --monitor
```

**2. Producer Only:**
```bash
python -m src.streaming.run_system --component producer
```

**3. Consumer Only:**
```bash
python -m src.streaming.run_system --component consumer
```

**4. Dashboard Only:**
```bash
python -m src.streaming.run_system --component dashboard
```

**5. Monitoring Dashboard Only:**
```bash
python -m src.streaming.run_system --component monitor
```

### Testing the System

Integration tests can be run with:
```bash
python -m src.streaming.test_integration --data-volume small --rate normal --duration 60
```

Performance tests:
```bash
python -m src.streaming.test_integration --data-volume large --rate fast --duration 300
```

## Troubleshooting

### Common Issues

1. **Kafka Connection Issues**
   - Check if Kafka is running
   - Verify bootstrap server address
   - Check network connectivity

2. **Slow Performance**
   - Check disk I/O performance
   - Adjust batch size and throttle rate
   - Monitor CPU and memory usage

3. **Consumer Lag**
   - Increase consumer processing capacity
   - Reduce producer throughput
   - Check for processing bottlenecks

4. **Dashboard Not Updating**
   - Verify consumer is producing output files
   - Check auto-refresh settings
   - Check file permissions

### Logs

Log files for troubleshooting are stored in the project root:
- `run_system.log` - Main system log
- `kafka_producer.log` - Producer-specific log
- `kafka_consumer.log` - Consumer-specific log
- `integration_test.log` - Integration test log

## Extending the System

### Adding New Analytics

To add new analytics:
1. Extend the `StreamingAnalytics` class in `src/streaming/consumer/stream_analytics.py`
2. Add new metric collection methods
3. Update the dashboard UI in `src/tabs/streaming_tab/ui.py`

### Adding New Data Sources

To support new data sources:
1. Create a new reader class similar to `ParquetReader`
2. Implement the batch processing interface
3. Update the producer to use the new reader 