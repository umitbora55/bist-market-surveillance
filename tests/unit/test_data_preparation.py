"""
Unit tests for data preparation module
"""

import pytest
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from ml_models.data_preparation import DataPreparation


class TestDataPreparation:
    """Test cases for DataPreparation class"""
    
    @pytest.fixture
    def sample_data(self):
        """Create sample data for testing"""
        dates = pd.date_range(start='2025-10-01', end='2025-10-30', freq='D')
        data = []
        
        for symbol in ['THYAO', 'GARAN']:
            for date in dates:
                data.append({
                    'symbol': symbol,
                    'trade_date': date,
                    'open_price': 100.0 + np.random.randn(),
                    'close_price': 100.0 + np.random.randn(),
                    'high_price': 105.0 + np.random.randn(),
                    'low_price': 95.0 + np.random.randn(),
                    'total_volume': int(1000000 + np.random.randn() * 100000),
                    'trade_count': 100,
                    'price_change_pct': np.random.randn(),
                    'volatility': 0.02 + np.random.randn() * 0.005
                })
        
        return pd.DataFrame(data)
    
    def test_engineer_features(self, sample_data):
        """Test feature engineering"""
        prep = DataPreparation()
        result = prep.engineer_features(sample_data)
        
        # Check new features are created
        assert 'ma5_price' in result.columns
        assert 'ma5_volume' in result.columns
        assert 'price_momentum' in result.columns
        assert 'volume_ratio' in result.columns
        
        # Check no NaN in final result
        assert not result.isnull().any().any()
        
        # Check data types
        assert result['ma5_price'].dtype == np.float64
        assert result['volume_ratio'].dtype == np.float64
    
    def test_create_sequences(self, sample_data):
        """Test sequence creation"""
        prep = DataPreparation()
        df_engineered = prep.engineer_features(sample_data)
        
        X, symbols, features = prep.create_sequences(df_engineered, sequence_length=5)
        
        # Check output shapes
        assert X.ndim == 3
        assert X.shape[1] == 5  # sequence length
        assert X.shape[2] == 10  # number of features
        assert len(symbols) == X.shape[0]
        assert len(features) == 10
    
    def test_normalize_data(self):
        """Test data normalization"""
        prep = DataPreparation()
        
        # Create sample sequences
        X = np.random.randn(10, 5, 3) * 100 + 50
        
        X_normalized = prep.normalize_data(X)
        
        # Check shape preserved
        assert X_normalized.shape == X.shape
        
        # Check normalization (mean close to 0, std close to 1)
        for i in range(X.shape[2]):
            feature_mean = X_normalized[:, :, i].mean()
            feature_std = X_normalized[:, :, i].std()
            
            assert abs(feature_mean) < 0.5
            assert abs(feature_std - 1.0) < 0.5


class TestFeatureEngineering:
    """Test feature engineering functions"""
    
    def test_moving_average_calculation(self):
        """Test moving average calculation"""
        data = {
            'symbol': ['TEST'] * 10,
            'trade_date': pd.date_range('2025-10-01', periods=10),
            'close_price': [100.0] * 10,
            'total_volume': [1000] * 10,
            'open_price': [100.0] * 10,
            'high_price': [105.0] * 10,
            'low_price': [95.0] * 10,
            'trade_count': [100] * 10,
            'price_change_pct': [0.0] * 10,
            'volatility': [0.02] * 10
        }
        df = pd.DataFrame(data)
        
        prep = DataPreparation()
        result = prep.engineer_features(df)
        
        # MA5 should be close to actual price after warmup
        assert abs(result['ma5_price'].iloc[-1] - 100.0) < 0.1
        assert abs(result['ma5_volume'].iloc[-1] - 1000.0) < 0.1
    
    def test_price_momentum(self):
        """Test price momentum calculation"""
        data = {
            'symbol': ['TEST'] * 6,
            'trade_date': pd.date_range('2025-10-01', periods=6),
            'close_price': [100.0, 102.0, 104.0, 103.0, 105.0, 107.0],
            'total_volume': [1000] * 6,
            'open_price': [100.0] * 6,
            'high_price': [105.0] * 6,
            'low_price': [95.0] * 6,
            'trade_count': [100] * 6,
            'price_change_pct': [0.0] * 6,
            'volatility': [0.02] * 6
        }
        df = pd.DataFrame(data)
        
        prep = DataPreparation()
        result = prep.engineer_features(df)
        
        # Check momentum is calculated
        assert 'price_momentum' in result.columns
        # Momentum should be positive for increasing prices
        assert result['price_momentum'].iloc[-1] > 0


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
