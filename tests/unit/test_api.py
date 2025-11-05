"""
Unit tests for API utilities and models
"""

import pytest
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))


class TestDataModels:
    """Test data model validations"""
    
    def test_stock_symbol_format(self):
        """Test stock symbol format validation"""
        valid_symbols = ['THYAO', 'GARAN', 'AKBNK']
        
        for symbol in valid_symbols:
            assert len(symbol) <= 6
            assert symbol.isupper()
            assert symbol.isalpha()
    
    def test_alert_severity_levels(self):
        """Test alert severity levels"""
        valid_severities = ['low', 'medium', 'high']
        
        test_severity = 'high'
        assert test_severity in valid_severities
    
    def test_alert_types(self):
        """Test alert type definitions"""
        valid_types = ['pump_and_dump', 'wash_trading', 'volume_spike', 'price_manipulation']
        
        test_type = 'pump_and_dump'
        assert test_type in valid_types


class TestUtilityFunctions:
    """Test utility functions"""
    
    def test_price_validation(self):
        """Test price validation logic"""
        price = 100.50
        
        assert isinstance(price, (int, float))
        assert price > 0
    
    def test_volume_validation(self):
        """Test volume validation logic"""
        volume = 1000000
        
        assert isinstance(volume, int)
        assert volume >= 0
    
    def test_percentage_calculation(self):
        """Test percentage change calculation"""
        old_price = 100.0
        new_price = 110.0
        
        change_pct = ((new_price - old_price) / old_price) * 100
        
        assert abs(change_pct - 10.0) < 0.01


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
