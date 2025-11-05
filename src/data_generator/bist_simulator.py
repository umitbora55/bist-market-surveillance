"""
BIST Market Data Generator with Real Data
Uses Yahoo Finance to fetch real BIST stock prices and generates realistic trades
"""

import random
import time
from datetime import datetime, timedelta
from typing import Dict, List
import json
import yfinance as yf
import pandas as pd


class BISTSimulator:
    """Generates BIST stock market trades based on real market data"""
    
    def __init__(self):
        # Popular BIST stocks with .IS suffix for Yahoo Finance
        self.stocks = {
            'THYAO.IS': {'name': 'Turkish Airlines', 'volatility': 0.02},
            'GARAN.IS': {'name': 'Garanti BBVA', 'volatility': 0.015},
            'AKBNK.IS': {'name': 'Akbank', 'volatility': 0.015},
            'ISCTR.IS': {'name': 'Is Bankasi', 'volatility': 0.02},
            'SISE.IS': {'name': 'Sise Cam', 'volatility': 0.018},
            'TUPRS.IS': {'name': 'Tupras', 'volatility': 0.025},
            'EREGL.IS': {'name': 'Erdemir', 'volatility': 0.02},
            'SAHOL.IS': {'name': 'Sabanci Holding', 'volatility': 0.017},
            'PETKM.IS': {'name': 'Petkim', 'volatility': 0.022},
            'KCHOL.IS': {'name': 'Koc Holding', 'volatility': 0.016}
        }
        
        self.current_prices = {}
        self.base_prices = {}
        self.daily_volumes = {symbol: 0 for symbol in self.stocks.keys()}
        
        # Fetch initial prices
        self._fetch_current_prices()
    
    def _fetch_current_prices(self):
        """Fetch current prices from Yahoo Finance"""
        print("Fetching real BIST stock prices from Yahoo Finance...")
        
        for symbol in self.stocks.keys():
            try:
                ticker = yf.Ticker(symbol)
                hist = ticker.history(period='5d')
                
                if not hist.empty:
                    latest_price = hist['Close'].iloc[-1]
                    self.current_prices[symbol] = round(latest_price, 2)
                    self.base_prices[symbol] = round(latest_price, 2)
                    print(f"  {symbol}: {latest_price:.2f} TRY")
                else:
                    # Fallback to default if data not available
                    self.current_prices[symbol] = 100.0
                    self.base_prices[symbol] = 100.0
                    print(f"  {symbol}: Using default price (data unavailable)")
                    
            except Exception as e:
                print(f"  Error fetching {symbol}: {e}")
                self.current_prices[symbol] = 100.0
                self.base_prices[symbol] = 100.0
        
        print("Price fetch complete.\n")
    
    def generate_price_movement(self, symbol: str) -> float:
        """Generate realistic price movement using random walk"""
        volatility = self.stocks[symbol]['volatility']
        current_price = self.current_prices[symbol]
        base_price = self.base_prices[symbol]
        
        # Random walk with drift
        drift = random.uniform(-0.0005, 0.0005)
        shock = random.gauss(0, volatility)
        
        price_change = current_price * (drift + shock)
        new_price = current_price + price_change
        
        # Keep price within reasonable bounds (±30% from base)
        new_price = max(new_price, base_price * 0.7)
        new_price = min(new_price, base_price * 1.3)
        
        self.current_prices[symbol] = round(new_price, 2)
        return self.current_prices[symbol]
    
    def generate_volume(self, symbol: str) -> int:
        """Generate realistic trading volume"""
        base_volume = random.randint(5000, 50000)
        volume = int(base_volume * random.uniform(0.5, 2.0))
        
        self.daily_volumes[symbol] += volume
        return volume
    
    def generate_trade(self, symbol: str = None) -> Dict:
        """Generate a single trade"""
        if symbol is None:
            symbol = random.choice(list(self.stocks.keys()))
        
        price = self.generate_price_movement(symbol)
        volume = self.generate_volume(symbol)
        
        # Calculate price change percentage
        base_price = self.base_prices[symbol]
        price_change_pct = ((price - base_price) / base_price) * 100
        
        trade = {
            'symbol': symbol.replace('.IS', ''),  # Remove .IS for cleaner display
            'symbol_full': symbol,
            'name': self.stocks[symbol]['name'],
            'price': price,
            'volume': volume,
            'price_change_pct': round(price_change_pct, 2),
            'timestamp': datetime.now().isoformat(),
            'trade_id': f"{symbol.replace('.IS', '')}_{int(time.time() * 1000000)}"
        }
        
        return trade
    
    def generate_batch(self, count: int = 10) -> List[Dict]:
        """Generate multiple trades at once"""
        return [self.generate_trade() for _ in range(count)]
    
    def inject_anomaly(self, anomaly_type: str = 'pump') -> Dict:
        """Inject anomalous trading pattern for testing"""
        symbol = random.choice(list(self.stocks.keys()))
        
        if anomaly_type == 'pump':
            # Pump: sudden price increase with high volume
            self.current_prices[symbol] *= 1.20  # 20% jump
            volume = self.generate_volume(symbol) * 15
            
        elif anomaly_type == 'dump':
            # Dump: sudden price decrease with high volume
            self.current_prices[symbol] *= 0.80  # 20% drop
            volume = self.generate_volume(symbol) * 15
            
        elif anomaly_type == 'wash':
            # Wash trading: same price, extreme volume
            volume = self.generate_volume(symbol) * 30
            
        else:
            volume = self.generate_volume(symbol)
        
        price = round(self.current_prices[symbol], 2)
        base_price = self.base_prices[symbol]
        price_change_pct = ((price - base_price) / base_price) * 100
        
        trade = {
            'symbol': symbol.replace('.IS', ''),
            'symbol_full': symbol,
            'name': self.stocks[symbol]['name'],
            'price': price,
            'volume': volume,
            'price_change_pct': round(price_change_pct, 2),
            'timestamp': datetime.now().isoformat(),
            'trade_id': f"{symbol.replace('.IS', '')}_{int(time.time() * 1000000)}",
            'anomaly_type': anomaly_type
        }
        
        return trade
    
    def reset_prices(self):
        """Reset prices to base values"""
        self.current_prices = self.base_prices.copy()
        self.daily_volumes = {symbol: 0 for symbol in self.stocks.keys()}


if __name__ == '__main__':
    # Test the simulator
    simulator = BISTSimulator()
    
    print("\nGenerating 5 normal trades:")
    print("-" * 80)
    for trade in simulator.generate_batch(5):
        print(f"{trade['symbol']:8} | {trade['price']:8.2f} TRY | Vol: {trade['volume']:6} | Change: {trade['price_change_pct']:+6.2f}%")
    
    print("\n\nInjecting anomalies:")
    print("-" * 80)
    
    for anomaly_type in ['pump', 'dump', 'wash']:
        anomaly = simulator.inject_anomaly(anomaly_type)
        print(f"\n{anomaly_type.upper()} - {anomaly['symbol']:8} | {anomaly['price']:8.2f} TRY | Vol: {anomaly['volume']:8} | Change: {anomaly['price_change_pct']:+6.2f}%")
