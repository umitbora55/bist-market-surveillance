"""
Data Preparation for ML Models
Prepares historical trade data for anomaly detection
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Tuple
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.postgres_client import PostgresClient
from database.elasticsearch_client import ElasticsearchClient


class DataPreparation:
    """Prepares data for ML training"""
    
    def __init__(self):
        self.pg_client = PostgresClient()
        self.es_client = ElasticsearchClient()
    
    def fetch_historical_aggregations(self, days=30):
        """Fetch historical aggregated data from PostgreSQL"""
        query = """
            SELECT 
                symbol,
                trade_date,
                open_price,
                close_price,
                high_price,
                low_price,
                total_volume,
                trade_count,
                price_change_pct,
                volatility
            FROM daily_aggregations
            WHERE trade_date > NOW() - INTERVAL '%s days'
            ORDER BY symbol, trade_date
        """ % days
        
        results = self.pg_client.execute_query(query)
        df = pd.DataFrame(results)
        return df
    
    def engineer_features(self, df):
        """Create features for ML models"""
        # Convert decimal to float
        numeric_columns = ['open_price', 'close_price', 'high_price', 'low_price', 
                          'total_volume', 'trade_count', 'price_change_pct', 'volatility']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = df[col].astype(float)
        
        # Sort by symbol and date
        df = df.sort_values(['symbol', 'trade_date'])
        
        # Calculate rolling statistics per symbol
        for symbol in df['symbol'].unique():
            mask = df['symbol'] == symbol
            
            # 5-day moving averages
            df.loc[mask, 'ma5_price'] = df.loc[mask, 'close_price'].rolling(5).mean()
            df.loc[mask, 'ma5_volume'] = df.loc[mask, 'total_volume'].rolling(5).mean()
            
            # Price momentum
            df.loc[mask, 'price_momentum'] = df.loc[mask, 'close_price'].pct_change()
            
            # Volume ratio
            df.loc[mask, 'volume_ratio'] = df.loc[mask, 'total_volume'] / df.loc[mask, 'ma5_volume']
            
            # Volatility change
            df.loc[mask, 'volatility_change'] = df.loc[mask, 'volatility'].pct_change()
            
            # Price range
            df.loc[mask, 'price_range'] = (df.loc[mask, 'high_price'] - df.loc[mask, 'low_price']) / df.loc[mask, 'close_price']
        
        # Drop NaN values
        df = df.dropna()
        
        return df
    
    def create_sequences(self, df, sequence_length=10):
        """
        Create sequences for LSTM
        
        Args:
            df: DataFrame with features
            sequence_length: Number of time steps in each sequence
        
        Returns:
            X: Sequences (samples, time_steps, features)
            symbols: Corresponding symbols
        """
        features = [
            'close_price', 'total_volume', 'price_change_pct',
            'volatility', 'ma5_price', 'ma5_volume',
            'price_momentum', 'volume_ratio', 'volatility_change',
            'price_range'
        ]
        
        sequences = []
        symbol_list = []
        
        for symbol in df['symbol'].unique():
            symbol_df = df[df['symbol'] == symbol][features].values
            
            # Create sequences
            for i in range(len(symbol_df) - sequence_length + 1):
                sequences.append(symbol_df[i:i+sequence_length])
                symbol_list.append(symbol)
        
        X = np.array(sequences)
        return X, symbol_list, features
    
    def normalize_data(self, X):
        """Normalize sequences"""
        # Normalize each feature across all samples
        X_normalized = np.zeros_like(X)
        
        for i in range(X.shape[2]):  # For each feature
            feature_data = X[:, :, i]
            mean = feature_data.mean()
            std = feature_data.std()
            
            if std > 0:
                X_normalized[:, :, i] = (feature_data - mean) / std
            else:
                X_normalized[:, :, i] = feature_data - mean
        
        return X_normalized
    
    def prepare_training_data(self, sequence_length=10):
        """
        Complete data preparation pipeline
        
        Returns:
            X_train: Training sequences
            symbols: Corresponding symbols
            feature_names: Feature names
        """
        print("Fetching historical data...")
        df = self.fetch_historical_aggregations(days=30)
        
        if df.empty:
            print("No historical data available!")
            return None, None, None
        
        print(f"Found {len(df)} records for {df['symbol'].nunique()} symbols")
        
        print("Engineering features...")
        df = self.engineer_features(df)
        
        print(f"Creating sequences (length={sequence_length})...")
        X, symbols, features = self.create_sequences(df, sequence_length)
        
        print(f"Normalizing data...")
        X_normalized = self.normalize_data(X)
        
        print(f"Final dataset shape: {X_normalized.shape}")
        print(f"  Samples: {X_normalized.shape[0]}")
        print(f"  Time steps: {X_normalized.shape[1]}")
        print(f"  Features: {X_normalized.shape[2]}")
        
        return X_normalized, symbols, features


def main():
    """Test data preparation"""
    prep = DataPreparation()
    
    X, symbols, features = prep.prepare_training_data(sequence_length=10)
    
    if X is not None:
        print("\nData preparation successful!")
        print(f"Feature names: {features}")
        print(f"Unique symbols: {set(symbols)}")
        print(f"Sample shape: {X[0].shape}")
    else:
        print("\nInsufficient data for training")
        print("Run the system to collect more data first")


if __name__ == "__main__":
    main()
