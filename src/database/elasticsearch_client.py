"""
Elasticsearch Client
Handles connections and operations with Elasticsearch
"""

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from datetime import datetime
from typing import List, Dict
import os


class ElasticsearchClient:
    """Client for Elasticsearch operations"""
    
    def __init__(self, host='localhost', port=9200):
        self.host = host
        self.port = port
        self.es = Elasticsearch([f'http://{host}:{port}'])
        
        # Check connection
        if self.es.ping():
            print(f"Connected to Elasticsearch at {host}:{port}")
        else:
            raise Exception(f"Cannot connect to Elasticsearch at {host}:{port}")
    
    def index_trade(self, trade: Dict, index='bist-trades'):
        """Index a single trade document"""
        try:
            result = self.es.index(
                index=index,
                id=trade['trade_id'],
                document=trade
            )
            return result
        except Exception as e:
            print(f"Error indexing trade: {e}")
            return None
    
    def bulk_index_trades(self, trades: List[Dict], index='bist-trades'):
        """Bulk index multiple trades"""
        actions = [
            {
                '_index': index,
                '_id': trade['trade_id'],
                '_source': trade
            }
            for trade in trades
        ]
        
        try:
            success, failed = bulk(self.es, actions)
            return {'success': success, 'failed': failed}
        except Exception as e:
            print(f"Error bulk indexing: {e}")
            return {'success': 0, 'failed': len(trades)}
    
    def index_alert(self, alert: Dict, index='bist-alerts'):
        """Index an alert document"""
        try:
            result = self.es.index(
                index=index,
                id=alert['alert_id'],
                document=alert
            )
            return result
        except Exception as e:
            print(f"Error indexing alert: {e}")
            return None
    
    def search_trades(self, symbol=None, start_time=None, end_time=None, 
                     size=100, index='bist-trades'):
        """Search trades with filters"""
        query = {"bool": {"must": []}}
        
        if symbol:
            query["bool"]["must"].append({"term": {"symbol": symbol}})
        
        if start_time or end_time:
            time_range = {}
            if start_time:
                time_range["gte"] = start_time
            if end_time:
                time_range["lte"] = end_time
            query["bool"]["must"].append({"range": {"timestamp": time_range}})
        
        try:
            result = self.es.search(
                index=index,
                query=query,
                size=size,
                sort=[{"timestamp": {"order": "desc"}}]
            )
            return result['hits']['hits']
        except Exception as e:
            print(f"Error searching trades: {e}")
            return []
    
    def get_recent_alerts(self, symbol=None, alert_type=None, size=50, 
                         index='bist-alerts'):
        """Get recent alerts"""
        query = {"bool": {"must": []}}
        
        if symbol:
            query["bool"]["must"].append({"term": {"symbol": symbol}})
        
        if alert_type:
            query["bool"]["must"].append({"term": {"alert_type": alert_type}})
        
        try:
            result = self.es.search(
                index=index,
                query=query if query["bool"]["must"] else {"match_all": {}},
                size=size,
                sort=[{"detected_at": {"order": "desc"}}]
            )
            return result['hits']['hits']
        except Exception as e:
            print(f"Error getting alerts: {e}")
            return []
    
    def aggregate_by_symbol(self, index='bist-trades'):
        """Get aggregations by symbol"""
        try:
            result = self.es.search(
                index=index,
                size=0,
                aggs={
                    "symbols": {
                        "terms": {"field": "symbol", "size": 20},
                        "aggs": {
                            "avg_price": {"avg": {"field": "price"}},
                            "total_volume": {"sum": {"field": "volume"}},
                            "trade_count": {"value_count": {"field": "trade_id"}}
                        }
                    }
                }
            )
            return result['aggregations']['symbols']['buckets']
        except Exception as e:
            print(f"Error aggregating: {e}")
            return []


if __name__ == '__main__':
    # Test the client
    client = ElasticsearchClient()
    
    # Search recent trades
    print("\nRecent trades:")
    trades = client.search_trades(size=5)
    for trade in trades:
        print(f"  {trade['_source']['symbol']}: {trade['_source']['price']} TRY")
    
    # Get aggregations
    print("\nAggregations by symbol:")
    aggs = client.aggregate_by_symbol()
    for bucket in aggs[:5]:
        print(f"  {bucket['key']}: {bucket['doc_count']} trades, "
              f"avg price: {bucket['avg_price']['value']:.2f} TRY")
