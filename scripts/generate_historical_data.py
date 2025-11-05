"""
Generate Historical Data
Creates synthetic historical daily aggregations for ML training
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from database.postgres_client import PostgresClient
from datetime import datetime, timedelta
import random


def generate_historical_data(days=30):
    """Generate synthetic historical data"""
    
    pg_client = PostgresClient()
    
    # Get all stocks
    stocks = pg_client.get_all_stocks()
    
    print(f"Generating {days} days of historical data for {len(stocks)} stocks...")
    
    for stock in stocks:
        symbol = stock['symbol']
        base_price = random.uniform(50, 300)
        
        print(f"\n{symbol}: Starting price {base_price:.2f}")
        
        current_price = base_price
        
        for day in range(days, 0, -1):
            trade_date = (datetime.now() - timedelta(days=day)).date()
            
            # Random daily change
            daily_change = random.uniform(-0.03, 0.03)  # -3% to +3%
            open_price = current_price
            close_price = current_price * (1 + daily_change)
            
            # High and low
            high_price = max(open_price, close_price) * random.uniform(1.0, 1.02)
            low_price = min(open_price, close_price) * random.uniform(0.98, 1.0)
            
            # Volume
            base_volume = random.randint(500000, 5000000)
            
            # Volatility
            volatility = abs(daily_change) * random.uniform(0.8, 1.2)
            
            agg_data = {
                'symbol': symbol,
                'trade_date': trade_date,
                'open_price': round(open_price, 2),
                'close_price': round(close_price, 2),
                'high_price': round(high_price, 2),
                'low_price': round(low_price, 2),
                'total_volume': base_volume,
                'trade_count': random.randint(100, 500),
                'price_change_pct': round(daily_change * 100, 2),
                'volatility': round(volatility, 4)
            }
            
            pg_client.insert_daily_aggregation(agg_data)
            current_price = close_price
        
        print(f"  Generated {days} days, final price: {current_price:.2f}")
    
    print(f"\nHistorical data generation complete!")
    pg_client.close()


if __name__ == "__main__":
    generate_historical_data(days=30)
