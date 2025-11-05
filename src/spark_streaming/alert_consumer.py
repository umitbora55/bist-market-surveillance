"""
Alert Consumer
Consumes alerts from Kafka and writes to Elasticsearch and PostgreSQL
"""

import json
import sys
import os
from kafka import KafkaConsumer
from datetime import datetime

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.elasticsearch_client import ElasticsearchClient
from database.postgres_client import PostgresClient


class AlertConsumer:
    """Consumes and stores alerts"""
    
    def __init__(self, bootstrap_servers='localhost:9092', topic='bist.alerts'):
        self.topic = topic
        
        # Initialize Kafka consumer
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='alert-consumer-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        # Initialize database clients
        self.es_client = ElasticsearchClient()
        self.pg_client = PostgresClient()
        
        print(f"Alert Consumer initialized. Listening to topic: {topic}")
    
    def process_alert(self, alert):
        """Process a single alert"""
        try:
            # Index to Elasticsearch
            es_result = self.es_client.index_alert(alert)
            
            # Save to PostgreSQL
            pg_data = {
                'alert_id': alert['alert_id'],
                'symbol': alert['symbol'],
                'alert_type': alert['alert_type'],
                'severity': alert['severity'],
                'description': alert['description'],
                'price': alert.get('price'),
                'volume': alert.get('volume'),
                'threshold_value': alert.get('threshold_value'),
                'detected_at': alert['detected_at']
            }
            
            pg_result = self.pg_client.insert_alert(pg_data)
            
            return {
                'success': True,
                'elasticsearch': es_result,
                'postgresql': pg_result
            }
            
        except Exception as e:
            print(f"Error processing alert: {e}")
            return {'success': False, 'error': str(e)}
    
    def start_consuming(self):
        """Start consuming alerts"""
        print("\nStarting alert consumption... Press Ctrl+C to stop\n")
        print(f"{'Symbol':<10} {'Type':<20} {'Severity':<10} {'Time':<25}")
        print("-" * 70)
        
        alert_count = 0
        
        try:
            for message in self.consumer:
                alert = message.value
                
                # Process alert
                result = self.process_alert(alert)
                
                if result['success']:
                    alert_count += 1
                    
                    # Print to console
                    print(f"{alert['symbol']:<10} {alert['alert_type']:<20} "
                          f"{alert['severity']:<10} {alert['detected_at']:<25}")
                    
                    if alert_count % 10 == 0:
                        print(f"\n[INFO] Processed {alert_count} alerts\n")
                
        except KeyboardInterrupt:
            print(f"\n\nStopping consumer...")
            print(f"Total alerts processed: {alert_count}")
            self.consumer.close()
            self.pg_client.close()
    
    def get_alert_statistics(self):
        """Get alert statistics from PostgreSQL"""
        query = """
            SELECT 
                alert_type,
                severity,
                COUNT(*) as count,
                AVG(threshold_value) as avg_threshold
            FROM alert_history
            GROUP BY alert_type, severity
            ORDER BY count DESC
        """
        
        results = self.pg_client.execute_query(query)
        
        print("\n=== Alert Statistics ===")
        print(f"{'Type':<20} {'Severity':<10} {'Count':<10} {'Avg Threshold':<15}")
        print("-" * 60)
        
        for row in results:
            print(f"{row['alert_type']:<20} {row['severity']:<10} "
                  f"{row['count']:<10} {row.get('avg_threshold', 0):<15.2f}")


def main():
    """Main execution"""
    consumer = AlertConsumer()
    consumer.start_consuming()


if __name__ == '__main__':
    main()
