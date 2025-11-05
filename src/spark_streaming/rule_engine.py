"""
Rule-Based Anomaly Detection Engine
Detects market manipulation patterns using predefined rules
"""

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import uuid


class AnomalyRuleEngine:
    """Rule-based anomaly detection for market surveillance"""
    
    def __init__(self):
        self.rules = {
            'pump_and_dump': self.detect_pump_and_dump,
            'wash_trading': self.detect_wash_trading,
            'volume_spike': self.detect_volume_spike,
            'price_manipulation': self.detect_price_manipulation
        }
    
    @staticmethod
    def detect_pump_and_dump(agg_df, price_threshold=15.0, volume_multiplier=3.0):
        """
        Detect pump and dump patterns
        Criteria: Rapid price increase with high volume
        """
        
        alerts_df = agg_df.filter(
            (col("price_change_pct") > price_threshold) &
            (col("total_volume") > 100000)
        ).withColumn("alert_type", lit("pump_and_dump")) \
          .withColumn("severity", lit("high")) \
          .withColumn("description", 
                     concat(lit("Rapid price increase: "), 
                           format_number(col("price_change_pct"), 2),
                           lit("% with volume: "),
                           format_number(col("total_volume"), 0)))
        
        return alerts_df
    
    @staticmethod
    def detect_wash_trading(agg_df, volume_threshold=500000, volatility_threshold=0.001):
        """
        Detect wash trading patterns
        Criteria: Extremely high volume with minimal price movement
        """
        
        alerts_df = agg_df.filter(
            (col("total_volume") > volume_threshold) &
            (abs(col("price_change_pct")) < 1.0) &
            (col("volatility") < volatility_threshold)
        ).withColumn("alert_type", lit("wash_trading")) \
          .withColumn("severity", lit("high")) \
          .withColumn("description",
                     concat(lit("High volume ("),
                           format_number(col("total_volume"), 0),
                           lit(") with minimal price change")))
        
        return alerts_df
    
    @staticmethod
    def detect_volume_spike(agg_df, spike_threshold=1000000):
        """
        Detect unusual volume spikes
        Criteria: Volume exceeds threshold
        """
        
        alerts_df = agg_df.filter(
            col("total_volume") > spike_threshold
        ).withColumn("alert_type", lit("volume_spike")) \
          .withColumn("severity", lit("medium")) \
          .withColumn("description",
                     concat(lit("Volume spike detected: "),
                           format_number(col("total_volume"), 0),
                           lit(" trades")))
        
        return alerts_df
    
    @staticmethod
    def detect_price_manipulation(agg_df, price_drop_threshold=-15.0):
        """
        Detect potential price manipulation (dump)
        Criteria: Sharp price decline
        """
        
        alerts_df = agg_df.filter(
            col("price_change_pct") < price_drop_threshold
        ).withColumn("alert_type", lit("price_manipulation")) \
          .withColumn("severity", lit("high")) \
          .withColumn("description",
                     concat(lit("Sharp price decline: "),
                           format_number(col("price_change_pct"), 2),
                           lit("%")))
        
        return alerts_df
    
    def apply_all_rules(self, agg_df):
        """Apply all detection rules and combine alerts"""
        
        all_alerts = None
        
        for rule_name, rule_func in self.rules.items():
            alerts = rule_func(agg_df)
            
            if all_alerts is None:
                all_alerts = alerts
            else:
                all_alerts = all_alerts.union(alerts)
        
        # Add alert metadata
        if all_alerts is not None:
            all_alerts = all_alerts.withColumn(
                "alert_id",
                concat(lit("ALERT_"), col("symbol"), lit("_"), 
                      date_format(col("window_start"), "yyyyMMdd_HHmmss"))
            )
            all_alerts = all_alerts.withColumn(
                "detected_at",
                current_timestamp()
            )
        
        return all_alerts
    
    @staticmethod
    def format_alerts_for_kafka(alerts_df):
        """Format alerts for Kafka topic"""
        
        kafka_alerts = alerts_df.select(
            col("alert_id"),
            col("symbol"),
            col("alert_type"),
            col("severity"),
            col("description"),
            col("avg_price").alias("price"),
            col("total_volume").alias("volume"),
            col("price_change_pct").alias("threshold_value"),
            col("detected_at"),
            to_json(struct(
                col("window_start"),
                col("window_end"),
                col("trade_count"),
                col("volatility")
            )).alias("metadata")
        )
        
        return kafka_alerts


def example_usage():
    """Example of rule-based detection"""
    from trade_consumer import TradeStreamConsumer
    from aggregations import TradeAggregator
    
    print("Starting Rule-Based Anomaly Detection...")
    
    consumer = TradeStreamConsumer(app_name="BISTRuleEngine")
    
    # Create stream
    raw_stream = consumer.create_kafka_stream()
    trades_df = consumer.parse_trade_json(raw_stream)
    
    # Perform aggregations
    aggregator = TradeAggregator()
    agg_df = aggregator.windowed_aggregations(trades_df, "1 minute")
    
    # Apply anomaly detection rules
    rule_engine = AnomalyRuleEngine()
    alerts_df = rule_engine.apply_all_rules(agg_df)
    
    # Display alerts
    display_alerts = alerts_df.select(
        col("symbol"),
        col("alert_type"),
        col("severity"),
        col("description"),
        col("detected_at")
    )
    
    print("\n=== Real-Time Alert Monitor ===\n")
    
    query = display_alerts \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .start()
    
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\nStopping alert monitor...")
        query.stop()
        consumer.stop()


if __name__ == "__main__":
    example_usage()
