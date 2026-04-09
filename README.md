# 📈 Real-Time Financial Streaming Pipeline

A real-time stock market data pipeline that streams live prices from Alpha Vantage API through Apache Kafka into PostgreSQL, with a live Plotly dashboard updating every 30 seconds.

[![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat-square&logo=python&logoColor=white)](https://python.org)
[![Kafka](https://img.shields.io/badge/Apache_Kafka-7.4-231F20?style=flat-square&logo=apachekafka&logoColor=white)](https://kafka.apache.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?style=flat-square&logo=postgresql&logoColor=white)](https://postgresql.org)
[![Docker](https://img.shields.io/badge/Docker-29.3-2496ED?style=flat-square&logo=docker&logoColor=white)](https://docker.com)
[![Plotly](https://img.shields.io/badge/Plotly_Dash-6.6-3F4F75?style=flat-square&logo=plotly&logoColor=white)](https://plotly.com)

---

## Architecture

```
Alpha Vantage API       Kafka              Consumer          PostgreSQL
(Real stock prices) --> (Message     --> (Processes    --> (Data
AAPL, GOOGL, MSFT,      Broker)          batches +         Warehouse)
AMZN, TSLA              topic:           validates)
every 60 seconds        financial_              |
                        transactions            v
                                            SQLite
                                          (Backup)
                                                |
                                                v
                                        Plotly Dashboard
                                        (Live charts
                                         every 30s)
```

---

## Project Structure

```
financial-streaming-pipeline/
├── src/
│   ├── producer/
│   │   └── transaction_producer.py  # Alpha Vantage API + Kafka producer
│   ├── consumer/
│   │   └── stream_processor.py      # Kafka consumer + 6 quality checks
│   └── dashboard/
│       └── dashboard.py             # Live Plotly Dash dashboard
├── config/
│   └── config.yaml                  # All settings + API key
├── docker-compose.yml               # Kafka + Zookeeper
├── run_pipeline.py                  # Runs everything
├── requirements.txt
└── README.md
```

---

## Setup and Installation

### Prerequisites
- Python 3.11
- Docker Desktop
- Alpha Vantage API key (free at alphavantage.co)

### 1. Clone the repository
```bash
git clone https://github.com/saikrishna-devp/financial-streaming-pipeline.git
cd financial-streaming-pipeline
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Start Kafka and Zookeeper
```bash
docker-compose up -d
```

### 4. Start PostgreSQL
```bash
docker run --name financial-db \
  -e POSTGRES_USER=admin \
  -e POSTGRES_PASSWORD=admin123 \
  -e POSTGRES_DB=financial_dwh \
  -p 5432:5432 -d postgres:15
```

### 5. Configure API key
Edit config/config.yaml and add your Alpha Vantage API key:
```yaml
alphavantage:
  api_key: YOUR_API_KEY_HERE
  symbols:
    - AAPL
    - GOOGL
    - MSFT
    - AMZN
    - TSLA
```

### 6. Run the pipeline
```bash
python run_pipeline.py
```

### 7. Open the dashboard
```
http://localhost:8050
```

---

## Live Dashboard Results (April 8, 2026)

```
AAPL   ->  +$5.40   (gaining)
AMZN   ->  +$7.48   (gaining)
GOOGL  ->  +$11.86  (biggest gainer)
MSFT   ->  +$2.04   (gaining)
TSLA   ->  -$3.40   (losing)
```

---

## Data Quality Checks

Every batch of stock quotes goes through 6 validation checks before loading:

| Check | Description |
|---|---|
| No null symbols | Every quote must have a stock symbol |
| No null transaction IDs | Every record must be uniquely identifiable |
| Price is positive | Stock prices must be greater than zero |
| Volume is positive | Trading volume must be non-negative |
| Symbol is valid | Only configured symbols accepted |
| Timestamp parseable | All timestamps must be valid datetime format |

---

## Key Features

- Live API integration - Alpha Vantage REST API for real market data
- Kafka message broker - Decouples producer and consumer for reliability
- 6 data quality checks - Validates every batch before database load
- Dual storage - PostgreSQL (primary) + SQLite (backup)
- Live dashboard - Plotly Dash updating every 30 seconds
- Flagging system - Automatically flags stocks with change greater than $5
- Docker - Kafka and Zookeeper containerized for easy setup

---

## How It Works

**Producer:** Every 60 seconds, fetches real stock quotes from Alpha Vantage API for AAPL, GOOGL, MSFT, AMZN, and TSLA. Each quote is serialized as JSON and published to the Kafka topic.

**Consumer:** Continuously reads from Kafka, collects messages into batches, runs 6 data quality checks, filters invalid records, and loads clean data into PostgreSQL and SQLite.

**Dashboard:** Reads from SQLite every 30 seconds and renders live charts showing price movements, daily changes, and trading volumes across all 5 stocks.

---

## Connect

**Saikrishna Suryavamsham** - Senior Data Engineer - Tampa, FL

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0A66C2?style=flat-square&logo=linkedin&logoColor=white)](https://linkedin.com/in/sai207)
[![Email](https://img.shields.io/badge/Email-EA4335?style=flat-square&logo=gmail&logoColor=white)](mailto:krishnasv207@gmail.com)
