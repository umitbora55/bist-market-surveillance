-- BIST Market Surveillance Database Schema

-- Stocks reference table
CREATE TABLE IF NOT EXISTS stocks (
    symbol VARCHAR(10) PRIMARY KEY,
    symbol_full VARCHAR(20) UNIQUE NOT NULL,
    name VARCHAR(100) NOT NULL,
    sector VARCHAR(50),
    base_price DECIMAL(10, 2),
    volatility DECIMAL(5, 4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Daily trading aggregations
CREATE TABLE IF NOT EXISTS daily_aggregations (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) REFERENCES stocks(symbol),
    trade_date DATE NOT NULL,
    open_price DECIMAL(10, 2),
    close_price DECIMAL(10, 2),
    high_price DECIMAL(10, 2),
    low_price DECIMAL(10, 2),
    total_volume BIGINT,
    trade_count INTEGER,
    price_change_pct DECIMAL(6, 2),
    volatility DECIMAL(6, 4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, trade_date)
);

-- Alert history
CREATE TABLE IF NOT EXISTS alert_history (
    id SERIAL PRIMARY KEY,
    alert_id VARCHAR(50) UNIQUE NOT NULL,
    symbol VARCHAR(10) REFERENCES stocks(symbol),
    alert_type VARCHAR(20) NOT NULL,
    severity VARCHAR(10) NOT NULL,
    description TEXT,
    price DECIMAL(10, 2),
    volume INTEGER,
    threshold_value DECIMAL(10, 2),
    detected_at TIMESTAMP NOT NULL,
    investigated BOOLEAN DEFAULT FALSE,
    investigation_notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ML model versions
CREATE TABLE IF NOT EXISTS ml_model_versions (
    id SERIAL PRIMARY KEY,
    model_name VARCHAR(50) NOT NULL,
    version VARCHAR(20) NOT NULL,
    model_type VARCHAR(30) NOT NULL,
    accuracy DECIMAL(5, 4),
    precision_score DECIMAL(5, 4),
    recall DECIMAL(5, 4),
    f1_score DECIMAL(5, 4),
    training_date TIMESTAMP NOT NULL,
    active BOOLEAN DEFAULT TRUE,
    model_path TEXT,
    hyperparameters JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(model_name, version)
);

-- Indices for performance
CREATE INDEX idx_daily_agg_symbol_date ON daily_aggregations(symbol, trade_date DESC);
CREATE INDEX idx_alerts_symbol ON alert_history(symbol);
CREATE INDEX idx_alerts_detected_at ON alert_history(detected_at DESC);
CREATE INDEX idx_alerts_type ON alert_history(alert_type);
CREATE INDEX idx_ml_models_active ON ml_model_versions(model_name, active);

-- Insert initial stock data
INSERT INTO stocks (symbol, symbol_full, name, sector, volatility) VALUES
    ('THYAO', 'THYAO.IS', 'Turkish Airlines', 'Transportation', 0.02),
    ('GARAN', 'GARAN.IS', 'Garanti BBVA', 'Banking', 0.015),
    ('AKBNK', 'AKBNK.IS', 'Akbank', 'Banking', 0.015),
    ('ISCTR', 'ISCTR.IS', 'Is Bankasi', 'Banking', 0.02),
    ('SISE', 'SISE.IS', 'Sise Cam', 'Manufacturing', 0.018),
    ('TUPRS', 'TUPRS.IS', 'Tupras', 'Energy', 0.025),
    ('EREGL', 'EREGL.IS', 'Erdemir', 'Manufacturing', 0.02),
    ('SAHOL', 'SAHOL.IS', 'Sabanci Holding', 'Holding', 0.017),
    ('PETKM', 'PETKM.IS', 'Petkim', 'Chemicals', 0.022),
    ('KCHOL', 'KCHOL.IS', 'Koc Holding', 'Holding', 0.016)
ON CONFLICT (symbol) DO NOTHING;
