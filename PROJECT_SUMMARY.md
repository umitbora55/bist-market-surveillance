# BIST Market Surveillance - Project Summary

## Overview

A complete real-time market surveillance platform for Borsa Istanbul (BIST), designed for financial infrastructure institutions like TakasBank and MKK.

## Technology Architecture

### Data Pipeline
- **Ingestion:** Apache Kafka (3 topics)
- **Processing:** Apache Spark Structured Streaming
- **Storage:** Elasticsearch + PostgreSQL (dual storage)
- **Caching:** Redis
- **Orchestration:** Apache Airflow (ready)

### Machine Learning
- **LSTM Autoencoder:** Time-series anomaly detection via reconstruction error
- **Isolation Forest:** Multivariate anomaly detection
- **Training Data:** 30 days historical, 10 BIST stocks

### API & Visualization
- **REST API:** FastAPI with 8 endpoints
- **Dashboard:** Grafana with 4 real-time panels
- **Monitoring:** Health checks, metrics, alerts

## Anomaly Detection

### Rule-Based (4 Rules)
1. **Pump and Dump:** Price increase >15% with high volume
2. **Wash Trading:** High volume with minimal price change
3. **Volume Spike:** Unusual volume exceeding threshold
4. **Price Manipulation:** Sharp price decline >15%

### ML-Based (2 Models)
1. **LSTM Autoencoder**
   - Threshold: 1.2211
   - Detection rate: 1.22% on training data
   - Architecture: 64→32→64 LSTM units

2. **Isolation Forest**
   - Contamination: 10%
   - Detection rate: 10.37% on training data
   - Features: 100 (10 timesteps × 10 features)

## Performance Metrics

- **Processing Rate:** 20 trades/second
- **Alert Latency:** <2 seconds
- **Total Alerts Generated:** 96+
- **Monitored Stocks:** 10 BIST symbols
- **API Response Time:** <100ms
- **Dashboard Refresh:** Real-time (5s interval)

## Data Sources

Real-time market data from Yahoo Finance API:
- THYAO (Turkish Airlines)
- GARAN (Garanti BBVA)
- AKBNK (Akbank)
- ISCTR (Is Bankasi)
- SISE (Sise Cam)
- TUPRS (Tupras)
- EREGL (Erdemir)
- SAHOL (Sabanci Holding)
- PETKM (Petkim)
- KCHOL (Koc Holding)

## Project Statistics

- **Total Commits:** 14+
- **Lines of Code:** 3000+
- **Python Modules:** 20+
- **Docker Services:** 6
- **API Endpoints:** 8
- **Dashboard Panels:** 4
- **ML Models:** 2
- **Anomaly Rules:** 4

## Completed Sprints

### Sprint 1: Foundation 
- Infrastructure setup (Docker, Kafka, Elasticsearch, PostgreSQL, Redis, Grafana)
- Data pipeline (Producer, Consumer, Spark Streaming)
- Real-time aggregations (1-minute windows)
- Database clients and schema

### Sprint 2: Visualization & API 
- FastAPI REST API
- Grafana dashboard
- Alert consumer
- Complete documentation

### Sprint 3: Machine Learning 
- LSTM Autoencoder
- Isolation Forest
- Data preparation pipeline
- Model training and evaluation

## Business Value

### For TakasBank
- Real-time risk monitoring
- T+2 settlement anomaly detection
- Collateral management insights
- Regulatory compliance support

### For MKK
- Securities registry monitoring
- Trade validation
- Suspicious activity detection
- Audit trail support

### For SPK (Regulatory)
- Market manipulation detection
- Investor protection
- Compliance monitoring
- Real-time alerts

## Technical Highlights

1. **Scalability:** Kafka + Spark architecture supports 1000+ trades/sec
2. **Reliability:** Dual storage (Elasticsearch + PostgreSQL)
3. **Flexibility:** Rule-based + ML-based detection
4. **Observability:** Grafana dashboard + REST API
5. **Maintainability:** Clean code, documentation, version control

## Future Enhancements

1. **Airflow Integration:** Automated model retraining, data cleanup
2. **Advanced ML:** Transformer models, ensemble methods
3. **Real-time Alerts:** Email/SMS notifications
4. **Extended Coverage:** More BIST stocks, bonds, derivatives
5. **Performance Optimization:** Caching strategies, query optimization

## Installation & Usage

See README.md for detailed installation instructions.

Quick start:
```bash
docker-compose up -d
python src/kafka_producers/trade_producer.py --mode stream --rate 15
python src/spark_streaming/full_pipeline.py
python src/api/main.py
```

Access:
- Dashboard: http://localhost:3000
- API: http://localhost:8000/docs
- Elasticsearch: http://localhost:9200

## Contact

Ümit Bora Günaydın
- LinkedIn: https://www.linkedin.com/in/ümit-bora-günaydın
- Email: umitbora94@gmail.com

---

**Date:** November 2025
**Status:** Production Ready
