# Steam Reviews Streaming Dashboard

This document explains how the streaming functionality works in the Steam Reviews Analysis Dashboard.

## Overview

The dashboard supports two modes of operation:
1. **Batch Analysis Mode** - Reads data directly from Parquet files
2. **Real-time Streaming Mode** - Processes data from a Kafka streaming pipeline (with mock capability)

## How Streaming Works

When you switch from "Batch Analysis" to "Real-time Streaming" mode using the toggle in the sidebar:

1. The session state changes from "batch" to "streaming"
2. The system starts Kafka producer and consumer processes 
3. The producer reads data from Parquet files and sends it to Kafka
4. The consumer reads from Kafka and updates the dashboard in real-time
5. The system shows status indicators in the sidebar

## Mock Kafka Mode

The system includes a mock Kafka implementation for testing and development:

- No need for an actual Kafka installation
- Uses in-memory message queues to simulate Kafka topics
- Supports producer/consumer pattern and callbacks
- Enabled by setting `USE_MOCK_KAFKA=True` in settings

## Testing the Streaming Functionality

Several test scripts are available to verify the streaming functionality:

- `test_parquet_reader.py` - Tests reading data from Parquet files
- `test_producer_send.py` - Tests the producer's ability to send data to Kafka
- `debug_streaming.py` - Tests session state changes and streaming control
- `verify_streamlit_data_flow.py` - Verifies the complete data flow

## Running the Dashboard

To run the dashboard:

```bash
python -m streamlit run src/streamlit_app.py
```

To see debugging information:

1. Ensure `DEBUG_MODE = True` in `src/streamlit_app.py`
2. Go to the "Debug" tab in the dashboard

## Status Indicators

The dashboard shows the following status indicators in the sidebar:

- **Streaming Status**: On/Off
- **Producer Status**: Running/Stopped
- **Consumer Status**: Running/Stopped
- **Mock Mode**: Enabled/Disabled

## Troubleshooting

If streaming isn't working correctly:

1. Check the "Debug" tab for detailed information
2. Look for errors in the terminal/console output
3. Verify that the Parquet files exist in the correct location
4. Try running the test scripts to isolate the issue

## Implementation Details

The streaming functionality is implemented in several key components:

- `src/utils/stream_control.py` - Controls starting/stopping streaming processes
- `src/streaming/producer/kafka_producer.py` - Reads from Parquet and sends to Kafka
- `src/streaming/consumer/kafka_consumer.py` - Consumes from Kafka for dashboard updates
- `src/streaming/utils/mock_kafka.py` - Provides the mock Kafka implementation
- `src/tabs/streaming_tab/ui.py` - Renders the streaming analytics UI 