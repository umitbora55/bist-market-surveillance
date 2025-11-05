"""
Simple Trade to Elasticsearch Consumer
Reads trades from Kafka and writes to Elasticsearch
"""

import json
import sys
import os
from kafka import KafkaConsumer

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.elasticsearch_client import ElasticsearchClient


def main():
    # Initialize
    consumer = KafkaConsumer(
        'bist.trades.raw',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    es_client = ElasticsearchClient()
    
    print("Consuming trades and writing to Elasticsearch...")
    print("Press Ctrl+C to stop\n")
    
    count = 0
    for message in consumer:
        trade = message.value
        
        # Write to ES
        es_client.index_trade(trade)
        count += 1
        
        if count % 50 == 0:
            print(f"Indexed {count} trades")
        
        if count >= 500:  # Stop after 500 trades
            break
    
    print(f"\nTotal trades indexed: {count}")
    consumer.close()


if __name__ == "__main__":
    main()
