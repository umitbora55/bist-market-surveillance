"""
Kafka Trade Producer
Sends simulated BIST trades to Kafka topic
"""

import json
import time
import sys
import os
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_generator.bist_simulator import BISTSimulator


class TradeProducer:
    """Produces trade messages to Kafka"""
    
    def __init__(self, bootstrap_servers='localhost:9092', topic='bist.trades.raw'):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            retries=3
        )
        self.simulator = BISTSimulator()
        print(f"Kafka Producer initialized. Topic: {topic}")
    
    def send_trade(self, trade):
        """Send a single trade to Kafka"""
        try:
            # Use symbol as key for partitioning
            key = trade['symbol']
            future = self.producer.send(self.topic, key=key, value=trade)
            
            # Wait for send to complete
            record_metadata = future.get(timeout=10)
            
            return {
                'success': True,
                'topic': record_metadata.topic,
                'partition': record_metadata.partition,
                'offset': record_metadata.offset
            }
            
        except KafkaError as e:
            print(f"Error sending trade: {e}")
            return {'success': False, 'error': str(e)}
    
    def start_streaming(self, trades_per_second=10, anomaly_probability=0.05):
        """Start continuous streaming of trades"""
        print(f"\nStarting trade stream: {trades_per_second} trades/sec")
        print(f"Anomaly injection probability: {anomaly_probability * 100}%")
        print("Press Ctrl+C to stop\n")
        
        trade_count = 0
        anomaly_count = 0
        
        try:
            while True:
                # Decide if this should be an anomaly
                if random.random() < anomaly_probability:
                    anomaly_type = random.choice(['pump', 'dump', 'wash'])
                    trade = self.simulator.inject_anomaly(anomaly_type)
                    anomaly_count += 1
                    print(f"[ANOMALY] {trade['symbol']:8} | {trade['price']:8.2f} TRY | Type: {anomaly_type:5}")
                else:
                    trade = self.simulator.generate_trade()
                
                result = self.send_trade(trade)
                
                if result['success']:
                    trade_count += 1
                    if trade_count % 100 == 0:
                        print(f"Sent {trade_count} trades ({anomaly_count} anomalies)")
                
                # Control rate
                time.sleep(1.0 / trades_per_second)
                
        except KeyboardInterrupt:
            print(f"\n\nStopping producer...")
            print(f"Total trades sent: {trade_count}")
            print(f"Total anomalies: {anomaly_count}")
            self.producer.flush()
            self.producer.close()
    
    def send_batch(self, count=100):
        """Send a batch of trades"""
        print(f"Sending batch of {count} trades...")
        
        for i in range(count):
            trade = self.simulator.generate_trade()
            result = self.send_trade(trade)
            
            if (i + 1) % 10 == 0:
                print(f"Sent {i + 1}/{count} trades")
        
        self.producer.flush()
        print("Batch complete!")


if __name__ == '__main__':
    import random
    import argparse
    
    parser = argparse.ArgumentParser(description='BIST Trade Producer')
    parser.add_argument('--mode', choices=['stream', 'batch'], default='stream',
                       help='Production mode')
    parser.add_argument('--rate', type=int, default=10,
                       help='Trades per second (stream mode)')
    parser.add_argument('--count', type=int, default=100,
                       help='Number of trades (batch mode)')
    parser.add_argument('--topic', default='bist.trades.raw',
                       help='Kafka topic name')
    
    args = parser.parse_args()
    
    producer = TradeProducer(topic=args.topic)
    
    if args.mode == 'stream':
        producer.start_streaming(trades_per_second=args.rate)
    else:
        producer.send_batch(count=args.count)
