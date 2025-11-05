"""
Populate Daily Aggregations
Creates daily aggregation records from Elasticsearch trades
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from database.elasticsearch_client import ElasticsearchClient
from database.postgres_client import PostgresClient
from datetime import datetime, date
import pandas as pd


def populate_aggregations():
    """Create daily aggregations from ES trades"""
    
    es_client = ElasticsearchClient()
    pg_client = PostgresClient()
    
    print("Fetching all trades from Elasticsearch...")
    
    # Get all trades
    trades = es_client.es.search(
        index='bist-trades',
        body={
            "size": 10000,
            "query": {"match_all": {}},
            "sort": [{"timestamp": {"order": "asc"}}]
        }
    )
    
    if trades['hits']['total']['value'] == 0:
        print("No trades found in Elasticsearch")
        return
    
    # Convert to DataFrame
    trade_list = []
    for hit in trades['hits']['hits']:
        trade = hit['_source']
        trade_list.append({
            'symbol': trade['symbol'],
            'price': trade['price'],
            'volume': trade['volume'],
            'timestamp': pd.to_datetime(trade['timestamp'])
        })
    
    df = pd.DataFrame(trade_list)
    print(f"Found {len(df)} trades")
    
    # Group by symbol and date
    df['date'] = df['timestamp'].dt.date
    
    print("\nCreating daily aggregations...")
    
    for (symbol, trade_date), group in df.groupby(['symbol', 'date']):
        
        # Calculate aggregations
        agg_data = {
            'symbol': symbol,
            'trade_date': trade_date,
            'open_price': group['price'].iloc[0],
            'close_price': group['price'].iloc[-1],
            'high_price': group['price'].max(),
            'low_price': group['price'].min(),
            'total_volume': int(group['volume'].sum()),
            'trade_count': len(group),
            'price_change_pct': ((group['price'].iloc[-1] - group['price'].iloc[0]) / group['price'].iloc[0] * 100),
            'volatility': group['price'].std() / group['price'].mean() if group['price'].mean() > 0 else 0
        }
        
        # Insert to PostgreSQL
        success = pg_client.insert_daily_aggregation(agg_data)
        
        if success:
            print(f"  {symbol} - {trade_date}: {agg_data['trade_count']} trades, "
                  f"close: {agg_data['close_price']:.2f}")
    
    print("\nDaily aggregations created successfully!")
    pg_client.close()


if __name__ == "__main__":
    populate_aggregations()
