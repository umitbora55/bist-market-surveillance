"""
Complete Streaming Pipeline
Reads trades, detects anomalies, writes alerts to Kafka
"""

from trade_consumer import TradeStreamConsumer
from aggregations import TradeAggregator
from rule_engine import AnomalyRuleEngine
from pyspark.sql.functions import *


def main():
    """Run complete pipeline"""
    print("=== BIST Market Surveillance - Full Pipeline ===\n")
    
    # Initialize consumer
    consumer = TradeStreamConsumer(app_name="BISTFullPipeline")
    
    # Create trade stream
    print("1. Reading from Kafka topic: bist.trades.raw")
    raw_stream = consumer.create_kafka_stream()
    trades_df = consumer.parse_trade_json(raw_stream)
    trades_df = consumer.add_processing_columns(trades_df)
    
    # Perform aggregations
    print("2. Performing windowed aggregations (1 minute)")
    aggregator = TradeAggregator()
    agg_df = aggregator.windowed_aggregations(trades_df, "1 minute")
    
    # Detect anomalies
    print("3. Applying anomaly detection rules")
    rule_engine = AnomalyRuleEngine()
    alerts_df = rule_engine.apply_all_rules(agg_df)
    
    # Format alerts for Kafka
    kafka_alerts = rule_engine.format_alerts_for_kafka(alerts_df)
    
    # Write alerts to Kafka
    print("4. Writing alerts to Kafka topic: bist.alerts")
    
    kafka_alerts_stream = kafka_alerts.select(
        col("alert_id").alias("key"),
        to_json(struct("*")).alias("value")
    )
    
    alerts_to_kafka = kafka_alerts_stream \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "bist.alerts") \
        .option("checkpointLocation", "/tmp/kafka_alerts_checkpoint") \
        .start()
    
    # Display alerts to console
    print("5. Displaying alerts to console\n")
    print("=== Real-Time Alert Monitor ===\n")
    
    display_alerts = alerts_df.select(
        col("symbol"),
        col("alert_type"),
        col("severity"),
        format_number(col("avg_price"), 2).alias("price"),
        format_number(col("total_volume"), 0).alias("volume"),
        col("description")
    )
    
    console_query = display_alerts \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .start()
    
    print("\nPipeline running... Press Ctrl+C to stop\n")
    
    try:
        console_query.awaitTermination()
    except KeyboardInterrupt:
        print("\n\nStopping pipeline...")
        alerts_to_kafka.stop()
        console_query.stop()
        consumer.stop()
        print("Pipeline stopped successfully")


if __name__ == "__main__":
    main()
