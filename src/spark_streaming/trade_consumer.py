"""
Spark Structured Streaming Consumer
Reads trades from Kafka and performs real-time processing
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os


class TradeStreamConsumer:
    """Consumes and processes trade stream from Kafka"""
    
    def __init__(self, app_name="BISTTradeConsumer"):
        # Create Spark session
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.jars.packages", 
                   "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        print(f"Spark session created: {app_name}")
    
    def create_kafka_stream(self, bootstrap_servers="localhost:9092",
                           topic="bist.trades.raw"):
        """Create streaming DataFrame from Kafka"""
        
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "earliest") \
            .load()
        
        print(f"Connected to Kafka topic: {topic}")
        return df
    
    def parse_trade_json(self, df):
        """Parse JSON trade data from Kafka"""
        
        # Define schema for trade data
        trade_schema = StructType([
            StructField("symbol", StringType(), True),
            StructField("symbol_full", StringType(), True),
            StructField("name", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("volume", IntegerType(), True),
            StructField("price_change_pct", DoubleType(), True),
            StructField("timestamp", StringType(), True),
            StructField("trade_id", StringType(), True),
            StructField("anomaly_type", StringType(), True)
        ])
        
        # Parse JSON from Kafka value
        parsed_df = df.select(
            from_json(col("value").cast("string"), trade_schema).alias("data")
        ).select("data.*")
        
        # Convert timestamp string to timestamp type
        parsed_df = parsed_df.withColumn(
            "timestamp", 
            to_timestamp(col("timestamp"))
        )
        
        return parsed_df
    
    def add_processing_columns(self, df):
        """Add derived columns for processing"""
        
        df = df.withColumn("processing_time", current_timestamp())
        df = df.withColumn("hour", hour(col("timestamp")))
        df = df.withColumn("minute", minute(col("timestamp")))
        
        return df
    
    def print_stream_to_console(self, df, output_mode="append"):
        """Print stream to console for debugging"""
        
        query = df \
            .writeStream \
            .outputMode(output_mode) \
            .format("console") \
            .option("truncate", False) \
            .start()
        
        return query
    
    def write_to_kafka(self, df, topic="bist.trades.enriched"):
        """Write processed stream back to Kafka"""
        
        # Convert DataFrame to JSON
        kafka_df = df.select(
            col("symbol").alias("key"),
            to_json(struct("*")).alias("value")
        )
        
        query = kafka_df \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("topic", topic) \
            .option("checkpointLocation", "/tmp/kafka_checkpoint") \
            .start()
        
        return query
    
    def stop(self):
        """Stop Spark session"""
        self.spark.stop()
        print("Spark session stopped")


def main():
    """Main execution"""
    print("Starting BIST Trade Stream Consumer...")
    
    # Create consumer
    consumer = TradeStreamConsumer()
    
    # Create Kafka stream
    raw_stream = consumer.create_kafka_stream()
    
    # Parse JSON
    trades_df = consumer.parse_trade_json(raw_stream)
    
    # Add processing columns
    trades_df = consumer.add_processing_columns(trades_df)
    
    # Select columns to display
    display_df = trades_df.select(
        "symbol",
        "price",
        "volume",
        "price_change_pct",
        "timestamp"
    )
    
    # Print to console
    print("\nStarting stream processing... Press Ctrl+C to stop\n")
    query = consumer.print_stream_to_console(display_df)
    
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\nStopping stream...")
        query.stop()
        consumer.stop()


if __name__ == "__main__":
    main()
