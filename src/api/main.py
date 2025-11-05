"""
FastAPI Application
REST API for querying trades and alerts
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from typing import Optional, List
from datetime import datetime
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.elasticsearch_client import ElasticsearchClient
from database.postgres_client import PostgresClient

app = FastAPI(
    title="BIST Market Surveillance API",
    description="Real-time market surveillance and anomaly detection API",
    version="1.0.0"
)

# Initialize clients
es_client = ElasticsearchClient()
pg_client = PostgresClient()


@app.get("/")
def root():
    """Root endpoint"""
    return {
        "message": "BIST Market Surveillance API",
        "version": "1.0.0",
        "endpoints": {
            "health": "/health",
            "alerts": "/alerts",
            "alerts_stats": "/alerts/stats",
            "trades": "/trades",
            "stocks": "/stocks"
        }
    }


@app.get("/health")
def health_check():
    """Health check endpoint"""
    try:
        # Check Elasticsearch
        es_health = es_client.es.ping()
        
        # Check PostgreSQL
        pg_health = pg_client.execute_query("SELECT 1")
        
        return {
            "status": "healthy",
            "elasticsearch": "connected" if es_health else "disconnected",
            "postgresql": "connected" if pg_health else "disconnected",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Health check failed: {str(e)}")


@app.get("/alerts")
def get_alerts(
    symbol: Optional[str] = None,
    alert_type: Optional[str] = None,
    severity: Optional[str] = None,
    limit: int = Query(default=50, le=100)
):
    """Get recent alerts"""
    try:
        alerts = es_client.get_recent_alerts(
            symbol=symbol,
            alert_type=alert_type,
            size=limit
        )
        
        # Filter by severity if provided
        if severity:
            alerts = [a for a in alerts if a['_source'].get('severity') == severity]
        
        return {
            "count": len(alerts),
            "alerts": [alert['_source'] for alert in alerts]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching alerts: {str(e)}")


@app.get("/alerts/stats")
def get_alert_statistics():
    """Get alert statistics"""
    try:
        query = """
            SELECT 
                alert_type,
                severity,
                COUNT(*) as count,
                AVG(threshold_value) as avg_threshold,
                MIN(detected_at) as first_detected,
                MAX(detected_at) as last_detected
            FROM alert_history
            GROUP BY alert_type, severity
            ORDER BY count DESC
        """
        
        results = pg_client.execute_query(query)
        
        # Convert to dict
        stats = []
        for row in results:
            stats.append({
                'alert_type': row['alert_type'],
                'severity': row['severity'],
                'count': row['count'],
                'avg_threshold': float(row['avg_threshold']) if row['avg_threshold'] else 0,
                'first_detected': row['first_detected'].isoformat() if row['first_detected'] else None,
                'last_detected': row['last_detected'].isoformat() if row['last_detected'] else None
            })
        
        return {
            "statistics": stats,
            "total_alerts": sum(s['count'] for s in stats)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching statistics: {str(e)}")


@app.get("/alerts/{alert_id}")
def get_alert_by_id(alert_id: str):
    """Get specific alert by ID"""
    try:
        result = es_client.es.get(index='bist-alerts', id=alert_id)
        return result['_source']
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Alert not found: {str(e)}")


@app.get("/trades")
def get_trades(
    symbol: Optional[str] = None,
    limit: int = Query(default=100, le=500)
):
    """Get recent trades"""
    try:
        trades = es_client.search_trades(symbol=symbol, size=limit)
        
        return {
            "count": len(trades),
            "trades": [trade['_source'] for trade in trades]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching trades: {str(e)}")


@app.get("/stocks")
def get_stocks():
    """Get all stocks"""
    try:
        stocks = pg_client.get_all_stocks()
        
        # Convert to dict
        stock_list = []
        for stock in stocks:
            stock_list.append({
                'symbol': stock['symbol'],
                'symbol_full': stock['symbol_full'],
                'name': stock['name'],
                'sector': stock['sector'],
                'volatility': float(stock['volatility']) if stock['volatility'] else 0
            })
        
        return {
            "count": len(stock_list),
            "stocks": stock_list
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching stocks: {str(e)}")


@app.get("/stocks/{symbol}")
def get_stock(symbol: str):
    """Get specific stock"""
    try:
        stock = pg_client.get_stock(symbol.upper())
        
        if not stock:
            raise HTTPException(status_code=404, detail=f"Stock {symbol} not found")
        
        return {
            'symbol': stock['symbol'],
            'symbol_full': stock['symbol_full'],
            'name': stock['name'],
            'sector': stock['sector'],
            'volatility': float(stock['volatility']) if stock['volatility'] else 0
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching stock: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
