@"
# RTSED — Real-Time Stock Market Event Detection

A project for the **Stream Processing and Analytics** course demonstrating an end-to-end real-time event detection pipeline using Apache Kafka, Spark Structured Streaming, PostgreSQL and Streamlit.

## Project Objective

The objective is to detect significant stock-market events from 1-minute OHLCV data using a distributed streaming architecture.

The project demonstrates:

- Apache Kafka as the central streaming platform
- Kafka producers and consumers
- Multiple Kafka topics
- Kafka partitioning
- Consumer groups
- Window-based stream processing
- Aggregation and summarization
- Event detection
- Fault tolerance and recovery
- Persistent database storage
- Real-time analytics and visualization

---

## Architecture

```text
Historical Stock Dataset
          |
          v
   Python Kafka Producer
          |
          v
   Kafka: stock_data
      3 partitions
          |
          v
Spark Structured Streaming
          |
          v
  1-Minute Tumbling Window
          |
          v
     Aggregations
          |
          v
    Event Detection
   /              \
Price Spike      Volume Surge
          |
          v
   Kafka: stock_events
      3 partitions
          |
          v
      PostgreSQL
          |
          v
 Streamlit + Plotly
      Dashboard