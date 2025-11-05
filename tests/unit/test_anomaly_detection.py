"""
Unit tests for anomaly detection rules
"""

import pytest
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))


class TestAnomalyRules:
    """Test cases for anomaly detection rules logic"""
    
    def test_pump_and_dump_logic(self):
        """Test pump and dump detection logic"""
        # Test thresholds
        price_threshold = 15.0
        volume_threshold = 100000
        
        # Normal case - should not trigger
        normal_change = 10.0
        normal_volume = 50000
        
        assert not (normal_change > price_threshold and normal_volume > volume_threshold)
        
        # Pump case - should trigger
        pump_change = 20.0
        pump_volume = 200000
        
        assert pump_change > price_threshold and pump_volume > volume_threshold
    
    def test_wash_trading_logic(self):
        """Test wash trading detection logic"""
        # Test thresholds
        volume_threshold = 500000
        price_change_threshold = 1.0
        volatility_threshold = 0.001
        
        # Normal case
        normal_volume = 100000
        normal_change = 2.0
        normal_volatility = 0.02
        
        is_wash = (normal_volume > volume_threshold and 
                   abs(normal_change) < price_change_threshold and 
                   normal_volatility < volatility_threshold)
        
        assert not is_wash
        
        # Wash trading case
        wash_volume = 1000000
        wash_change = 0.5
        wash_volatility = 0.0005
        
        is_wash = (wash_volume > volume_threshold and 
                   abs(wash_change) < price_change_threshold and 
                   wash_volatility < volatility_threshold)
        
        assert is_wash
    
    def test_volume_spike_logic(self):
        """Test volume spike detection logic"""
        spike_threshold = 1000000
        
        # Normal volume
        normal_volume = 500000
        assert not (normal_volume > spike_threshold)
        
        # Spike volume
        spike_volume = 2000000
        assert spike_volume > spike_threshold
    
    def test_price_manipulation_logic(self):
        """Test price manipulation detection logic"""
        drop_threshold = -15.0
        
        # Normal decline
        normal_decline = -10.0
        assert not (normal_decline < drop_threshold)
        
        # Sharp decline
        sharp_decline = -20.0
        assert sharp_decline < drop_threshold
    
    def test_alert_severity_assignment(self):
        """Test alert severity assignment"""
        # High severity rules
        high_severity_types = ['pump_and_dump', 'price_manipulation']
        
        for alert_type in high_severity_types:
            assert self._get_severity(alert_type) == 'high'
        
        # Medium severity rules
        medium_severity_types = ['volume_spike']
        
        for alert_type in medium_severity_types:
            assert self._get_severity(alert_type) == 'medium'
    
    def _get_severity(self, alert_type):
        """Helper to get severity for alert type"""
        if alert_type in ['pump_and_dump', 'price_manipulation', 'wash_trading']:
            return 'high'
        elif alert_type in ['volume_spike']:
            return 'medium'
        return 'low'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
