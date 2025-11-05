"""
Real-time Aggregations
Performs windowed aggregations on trade streams
"""

from pyspark.sql.functions import *
from pyspark.sql.window import Window


class TradeAggregator:
    """Performs aggregations on trade streams"""
    
    @staticmethod
    def windowed_aggregations(df, window_duration="1 minute", 
                             watermark_delay="30 seconds"):
        """
        Perform windowed aggregations
        
        Args:
            df: Streaming DataFrame with trades
            window_duration: Window size (e.g., "1 minute", "5 minutes")
            watermark_delay: How late data can arrive
        """
        
        # Add watermark for handling late data
        df_with_watermark = df.withWatermark("timestamp", watermark_delay)
        
        # Aggregate by symbol and time window
        agg_df = df_with_watermark \
            .groupBy(
                col("symbol"),
                window(col("timestamp"), window_duration)
            ) \
            .agg(
                count("*").alias("trade_count"),
                sum("volume").alias("total_volume"),
                avg("price").alias("avg_price"),
                min("price").alias("min_price"),
                max("price").alias("max_price"),
                stddev("price").alias("price_stddev"),
                first("price").alias("open_price"),
                last("price").alias("close_price")
            )
        
        # Calculate price change percentage within window
        agg_df = agg_df.withColumn(
            "price_change_pct",
            ((col("close_price") - col("open_price")) / col("open_price") * 100)
        )
        
        # Calculate volatility (coefficient of variation)
        agg_df = agg_df.withColumn(
            "volatility",
            when(col("avg_price") > 0, 
                 col("price_stddev") / col("avg_price"))
            .otherwise(0)
        )
        
        # Extract window start and end times
        agg_df = agg_df.withColumn("window_start", col("window.start"))
        agg_df = agg_df.withColumn("window_end", col("window.end"))
        
        # Drop the window column
        agg_df = agg_df.drop("window")
        
        return agg_df
    
    @staticmethod
    def symbol_level_stats(df):
        """Calculate running statistics per symbol"""
        
        # Define window for running calculations
        window_spec = Window.partitionBy("symbol").orderBy("timestamp") \
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        
        stats_df = df.withColumn(
            "cumulative_volume",
            sum("volume").over(window_spec)
        )
        
        stats_df = stats_df.withColumn(
            "running_avg_price",
            avg("price").over(window_spec)
        )
        
        stats_df = stats_df.withColumn(
            "trade_sequence",
            row_number().over(window_spec)
        )
        
        return stats_df
    
    @staticmethod
    def detect_volume_spikes(agg_df, spike_threshold=50000):
        """
        Detect volume spikes in aggregated data (simplified for streaming)
        
        Args:
            agg_df: Aggregated DataFrame
            spike_threshold: Absolute volume threshold
        """
        
        # Simple threshold-based detection
        spike_df = agg_df.withColumn(
            "volume_spike",
            when(col("total_volume") > spike_threshold, True).otherwise(False)
        )
        
        spike_df = spike_df.withColumn(
            "volume_spike_ratio",
            col("total_volume") / lit(spike_threshold)
        )
        
        return spike_df
    
    @staticmethod
    def format_for_display(agg_df):
        """Format aggregated data for console display"""
        
        display_df = agg_df.select(
            col("symbol"),
            col("window_start"),
            col("trade_count"),
            format_number(col("total_volume"), 0).alias("volume"),
            format_number(col("avg_price"), 2).alias("avg_price"),
            format_number(col("price_change_pct"), 2).alias("change_%"),
            format_number(col("volatility"), 4).alias("volatility")
        )
        
        return display_df


def example_usage():
    """Example of how to use aggregations"""
    from trade_consumer import TradeStreamConsumer
    
    print("Starting aggregation example...")
    
    consumer = TradeStreamConsumer(app_name="BISTAggregations")
    
    # Create stream
    raw_stream = consumer.create_kafka_stream()
    trades_df = consumer.parse_trade_json(raw_stream)
    
    # Perform 1-minute aggregations
    aggregator = TradeAggregator()
    agg_df = aggregator.windowed_aggregations(trades_df, "1 minute")
    
    # Detect volume spikes
    agg_with_spikes = aggregator.detect_volume_spikes(agg_df)
    
    # Format for display
    display_df = aggregator.format_for_display(agg_with_spikes)
    
    # Print to console
    query = display_df \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .start()
    
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\nStopping...")
        query.stop()
        consumer.stop()


if __name__ == "__main__":
    example_usage()
