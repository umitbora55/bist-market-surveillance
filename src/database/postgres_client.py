"""
PostgreSQL Client
Handles connections and operations with PostgreSQL
"""

import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, date
from typing import List, Dict, Optional
import os


class PostgresClient:
    """Client for PostgreSQL operations"""
    
    def __init__(self, host='localhost', port=5432, database='bist_surveillance',
                 user='bist_user', password='bist_pass'):
        self.conn_params = {
            'host': host,
            'port': port,
            'database': database,
            'user': user,
            'password': password
        }
        self.conn = None
        self.connect()
    
    def connect(self):
        """Establish database connection"""
        try:
            self.conn = psycopg2.connect(**self.conn_params)
            print(f"Connected to PostgreSQL: {self.conn_params['database']}")
        except Exception as e:
            print(f"Error connecting to PostgreSQL: {e}")
            raise
    
    def execute_query(self, query: str, params: tuple = None, fetch=True):
        """Execute a query and optionally fetch results"""
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, params)
                if fetch:
                    return cur.fetchall()
                self.conn.commit()
                return cur.rowcount
        except Exception as e:
            self.conn.rollback()
            print(f"Error executing query: {e}")
            return None
    
    def get_all_stocks(self) -> List[Dict]:
        """Get all stocks"""
        query = "SELECT * FROM stocks ORDER BY symbol"
        return self.execute_query(query)
    
    def get_stock(self, symbol: str) -> Optional[Dict]:
        """Get a single stock by symbol"""
        query = "SELECT * FROM stocks WHERE symbol = %s"
        result = self.execute_query(query, (symbol,))
        return result[0] if result else None
    
    def insert_daily_aggregation(self, agg_data: Dict) -> bool:
        """Insert daily aggregation data"""
        query = """
            INSERT INTO daily_aggregations 
            (symbol, trade_date, open_price, close_price, high_price, 
             low_price, total_volume, trade_count, price_change_pct, volatility)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, trade_date) 
            DO UPDATE SET
                close_price = EXCLUDED.close_price,
                high_price = GREATEST(daily_aggregations.high_price, EXCLUDED.high_price),
                low_price = LEAST(daily_aggregations.low_price, EXCLUDED.low_price),
                total_volume = daily_aggregations.total_volume + EXCLUDED.total_volume,
                trade_count = daily_aggregations.trade_count + EXCLUDED.trade_count,
                price_change_pct = EXCLUDED.price_change_pct,
                volatility = EXCLUDED.volatility
        """
        params = (
            agg_data['symbol'],
            agg_data['trade_date'],
            agg_data['open_price'],
            agg_data['close_price'],
            agg_data['high_price'],
            agg_data['low_price'],
            agg_data['total_volume'],
            agg_data['trade_count'],
            agg_data.get('price_change_pct', 0),
            agg_data.get('volatility', 0)
        )
        return self.execute_query(query, params, fetch=False) is not None
    
    def get_daily_aggregations(self, symbol: str, days: int = 30) -> List[Dict]:
        """Get daily aggregations for a symbol"""
        query = """
            SELECT * FROM daily_aggregations 
            WHERE symbol = %s 
            ORDER BY trade_date DESC 
            LIMIT %s
        """
        return self.execute_query(query, (symbol, days))
    
    def insert_alert(self, alert_data: Dict) -> bool:
        """Insert an alert"""
        query = """
            INSERT INTO alert_history 
            (alert_id, symbol, alert_type, severity, description, 
             price, volume, threshold_value, detected_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (alert_id) DO NOTHING
        """
        params = (
            alert_data['alert_id'],
            alert_data['symbol'],
            alert_data['alert_type'],
            alert_data['severity'],
            alert_data.get('description', ''),
            alert_data.get('price'),
            alert_data.get('volume'),
            alert_data.get('threshold_value'),
            alert_data['detected_at']
        )
        return self.execute_query(query, params, fetch=False) is not None
    
    def get_recent_alerts(self, symbol: str = None, limit: int = 50) -> List[Dict]:
        """Get recent alerts"""
        if symbol:
            query = """
                SELECT * FROM alert_history 
                WHERE symbol = %s 
                ORDER BY detected_at DESC 
                LIMIT %s
            """
            return self.execute_query(query, (symbol, limit))
        else:
            query = """
                SELECT * FROM alert_history 
                ORDER BY detected_at DESC 
                LIMIT %s
            """
            return self.execute_query(query, (limit,))
    
    def save_model_version(self, model_data: Dict) -> bool:
        """Save ML model version"""
        query = """
            INSERT INTO ml_model_versions 
            (model_name, version, model_type, accuracy, precision_score, 
             recall, f1_score, training_date, model_path, hyperparameters)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = (
            model_data['model_name'],
            model_data['version'],
            model_data['model_type'],
            model_data.get('accuracy'),
            model_data.get('precision_score'),
            model_data.get('recall'),
            model_data.get('f1_score'),
            model_data['training_date'],
            model_data.get('model_path'),
            model_data.get('hyperparameters')
        )
        return self.execute_query(query, params, fetch=False) is not None
    
    def get_active_model(self, model_name: str) -> Optional[Dict]:
        """Get active model version"""
        query = """
            SELECT * FROM ml_model_versions 
            WHERE model_name = %s AND active = TRUE 
            ORDER BY training_date DESC 
            LIMIT 1
        """
        result = self.execute_query(query, (model_name,))
        return result[0] if result else None
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            print("PostgreSQL connection closed")


if __name__ == '__main__':
    # Test the client
    client = PostgresClient()
    
    # Get all stocks
    print("\nAll stocks in database:")
    stocks = client.get_all_stocks()
    for stock in stocks:
        print(f"  {stock['symbol']}: {stock['name']} ({stock['sector']})")
    
    # Get specific stock
    print("\nGetting THYAO details:")
    thyao = client.get_stock('THYAO')
    print(f"  {thyao}")
    
    client.close()
