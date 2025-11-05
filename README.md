# BIST Real-Time Market Surveillance Platform

Real-time market surveillance and anomaly detection platform for Borsa Istanbul (BIST).

Türkiye Borsası İstanbul (BIST) için gerçek zamanlı piyasa gözetim ve anomali tespit platformu.

## Project Purpose / Proje Amacı

Addresses critical needs for financial infrastructure institutions like TakasBank and MKK:
- Real-time trade monitoring
- Market manipulation detection (pump-and-dump, wash trading, spoofing)
- Risk analysis and early warning system

TakasBank ve MKK gibi finansal altyapı kurumlarının kritik ihtiyaçlarına yanıt verir:
- Gerçek zamanlı işlem izleme
- Piyasa manipülasyonu tespiti (pump-and-dump, wash trading, spoofing)
- Risk analizi ve erken uyarı sistemi

## Project Status / Proje Durumu

Development Phase - Sprint 1 (Foundation Setup)

Geliştirme Aşaması - Sprint 1 (Temel Altyapı Kurulumu)

## Technology Stack / Teknoloji Stack

- **Streaming:** Apache Kafka 3.6
- **Processing:** Apache Spark 3.5
- **Storage:** Elasticsearch 8.11, PostgreSQL 15, Redis 7.2
- **Visualization:** Grafana 10.2
- **ML:** TensorFlow 2.15, Scikit-Learn 1.3
- **API:** FastAPI
- **Orchestration:** Apache Airflow

## Setup / Kurulum

### Requirements / Gereksinimler
- Docker and Docker Compose
- Python 3.10+
- Ubuntu 20.04+ (recommended / önerilir)

### Quick Start / Hızlı Başlangıç

1. Clone the repository / Repository'yi klonlayın:
```bash
git clone https://github.com/umitbora55/bist-market-surveillance.git
cd bist-market-surveillance
```

2. Setup environment variables / Environment variables'ı ayarlayın:
```bash
cp .env.example .env
```

3. Start Docker containers / Docker container'larını başlatın:
```bash
docker-compose up -d
```

4. Setup Python environment / Python environment'ı kurun:
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Architecture / Mimari

Coming soon / Yakında eklenecek

## License / Lisans
MIT License

## Contact / İletişim
Ümit Bora Günaydın - [LinkedIn](https://www.linkedin.com/in/ümit-bora-günaydın)
