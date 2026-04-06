# NSE Stock Market Analytics Pipeline


An end-to-end real-time data pipeline that ingests live OHLCV data for 50+ NSE tickers, processes it through a Medallion architecture on AWS S3, transforms it with dbt Core, orchestrates daily runs via Apache Airflow, and visualizes insights on a live Streamlit dashboard.

**Live Dashboard:** [stock-pipeline.streamlit.app](https://your-url.streamlit.app)

---

## Architecture

```
yfinance (NSE data)
        │
        ▼
  Kafka Producer
  (1-min OHLCV, 50 tickers)
        │
        ▼
  Apache Kafka
  (topic: stock_ticks, 3 partitions)
        │
        ▼
  Kafka Consumer
        │
        ▼
  AWS S3 — Bronze Layer
  (raw JSON, partitioned by date/ticker)
        │
        ▼
  AWS S3 — Silver Layer
  (cleaned Parquet, typed + deduplicated)
        │
        ▼
  AWS S3 — Gold Layer
  (daily OHLCV summary per ticker)
        │
        ▼
  AWS Redshift Serverless
  (staging_stock_ticks, dim_companies)
        │
        ▼
  dbt Core
  (10+ models: RSI, MA, volume anomaly)
        │
        ▼
  Streamlit Cloud Dashboard
  (live public URL)
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Data Source | yfinance (Yahoo Finance API) |
| Message Queue | Apache Kafka (Docker) |
| Data Lake | AWS S3 (Medallion Architecture) |
| Data Warehouse | AWS Redshift Serverless |
| Transformation | dbt Core |
| Orchestration | Apache Airflow (Docker) |
| Dashboard | Streamlit Cloud |
| Language | Python 3.11 |
| Infrastructure | Docker, Docker Compose |

---

## Project Structure

```
stock-pipeline/
├── ingestion/
│   ├── producer.py          # yfinance → Kafka producer
│   └── validate_tickers.py  # ticker validation script
├── kafka_consumer/
│   ├── consumer.py          # Kafka → S3 Bronze consumer
│   └── kafka_config.py      # Kafka producer/consumer config
├── s3/
│   ├── s3_helper.py         # S3 client + key helpers
│   ├── bronze_to_silver.py  # Bronze → Silver transformation
│   ├── silver_to_gold.py    # Silver → Gold aggregation
│   ├── load_to_redshift.py  # Gold → Redshift COPY
│   ├── create_tables.py     # Redshift table creation
│   └── load_companies.py    # dim_companies population
├── stock_pipeline/          # dbt project
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_stock_ticks.sql
│   │   │   └── sources.yml
│   │   └── marts/
│   │       ├── mart_daily_ohlcv.sql
│   │       ├── mart_moving_averages.sql
│   │       ├── mart_rsi.sql
│   │       ├── mart_volume_anomaly.sql
│   │       └── schema.yml
│   ├── dbt_project.yml
│   └── profiles.yml
├── dags/
│   └── stock_pipeline_dag.py  # Airflow DAGs
├── streamlit/
│   ├── app.py               # Streamlit dashboard
│   └── requirements.txt
├── docker-compose.yml       # Kafka + Airflow containers
├── Dockerfile.airflow       # Custom Airflow image
├── requirements.txt
└── .env                     # credentials (not committed)
```

---

## Data Pipeline

### Tickers — 50 NSE Stocks across 10 Sectors

| Sector | Tickers |
|---|---|
| IT & Technology | TCS, Infosys, Wipro, HCL Tech, Tech Mahindra, LTIMindtree, Mphasis, Persistent, Coforge, OFSS |
| Banking | HDFC Bank, ICICI Bank, Kotak Bank, SBI, Axis Bank, IndusInd, Bandhan, Federal Bank |
| Finance & NBFC | Bajaj Finance, Bajaj Finserv, HDFC Life, SBI Life, Cholamandalam |
| Oil & Energy | Reliance, ONGC, IOC, BPCL, Power Grid |
| Automobile | Maruti, Tata Motors CV, M&M, Bajaj Auto, Eicher Motors |
| Pharma | Sun Pharma, Dr Reddy's, Cipla, Divi's Lab, Apollo Hospitals |
| FMCG | HUL, ITC, Nestle, Britannia, Dabur |
| Metals & Mining | Tata Steel, JSW Steel, Hindalco, Coal India |
| Infrastructure | UltraTech, Grasim, Adani Ports, L&T |
| Telecom & Media | Bharti Airtel, Vodafone Idea, Zee Entertainment |

### Data Collected per Ticker

```json
{
  "ticker":      "TCS.NS",
  "timestamp":   "2026-04-06T09:16:00+05:30",
  "open_price":  3812.50,
  "high_price":  3819.00,
  "low_price":   3810.00,
  "close_price": 3815.75,
  "volume":      14250,
  "ingested_at": "2026-04-06T03:46:05.123Z"
}
```

### Medallion Architecture

| Layer | Format | Content |
|---|---|---|
| Bronze | JSON | Raw tick events, partitioned by `date/ticker` |
| Silver | Parquet | Cleaned, typed, deduplicated 1-min candles |
| Gold | Parquet | Daily OHLCV summary + computed indicators |

### Computed Indicators

| Indicator | Description |
|---|---|
| MA 7 | 7-period moving average of close price |
| MA 20 | 20-period moving average of close price |
| RSI 14 | 14-period Relative Strength Index |
| Daily Return | `(close - open) / open × 100` |
| Volume Anomaly | `True` if volume > 2× average daily volume |

---

## dbt Models

```
staging
└── stg_stock_ticks        (view)  — clean + cast staging table

marts
├── mart_daily_ohlcv       (table) — OHLCV + return direction signal
├── mart_moving_averages   (table) — MA7, MA20 + bullish/bearish signal
├── mart_rsi               (table) — RSI14 + overbought/oversold signal
└── mart_volume_anomaly    (table) — tickers with abnormal volume
```

All mart models have schema tests — `not_null`, `unique`, `accepted_values`.

---

## Airflow DAGs

### DAG 1 — `stock_intraday_producer`
- **Schedule:** Every minute, Mon–Fri, 9:15–15:30 IST
- **Tasks:** `fetch_and_produce` → `consume_to_bronze`
- Collects all 390 intraday candles throughout the trading day

### DAG 2 — `stock_daily_processing`
- **Schedule:** 4:00 PM IST, Mon–Fri (after market close)
- **Tasks:** `bronze_to_silver` → `silver_to_gold` → `load_to_redshift` → `dbt_run` → `dbt_test`
- Processes and loads the full day's data into Redshift

Both DAGs have retry logic (2 retries, 5-minute delay) on every task.

---

## Streamlit Dashboard Pages

| Page | Description |
|---|---|
| Market Overview | Gainers, losers, sector performance heatmap |
| Stock Analysis | Candlestick chart with MA7/MA20 overlay + volume |
| RSI Signals | RSI bar chart with overbought/oversold zones |
| Volume Anomalies | Tickers with unusual trading activity |
| Pipeline Health | Row counts, data freshness, sector distribution |

---

## Setup Instructions

### Prerequisites
- Python 3.11+
- Docker Desktop
- AWS account (S3 + Redshift Serverless)

### 1. Clone the repo

```bash
git clone https://github.com/Pratham-Jethwani/stock-pipeline.git
cd stock-pipeline
```

### 2. Create virtual environment

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Configure environment variables

```bash
cp .env.example .env
```

Fill in your credentials:

```bash
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_REGION=ap-south-1
S3_BUCKET_NAME=your_bucket

KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=stock_ticks

REDSHIFT_HOST=your_redshift_host
REDSHIFT_PORT=5439
REDSHIFT_DB=dev
REDSHIFT_USER=admin
REDSHIFT_PASSWORD=your_password
REDSHIFT_IAM_ROLE_ARN=arn:aws:iam::xxxx:role/redshift-s3-role
```

### 4. Start Kafka and Airflow

```bash
docker compose up -d
```

### 5. Create Kafka topic

```bash
docker exec kafka kafka-topics \
  --create \
  --topic stock_ticks \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

### 6. Set up Redshift tables

```bash
python s3/create_tables.py
python s3/load_companies.py
```

### 7. Set up dbt

```bash
cd stock_pipeline
dbt debug
dbt run
dbt test
```

### 8. Run the pipeline manually

```bash
# terminal 1
python ingestion/producer.py

# terminal 2
python kafka_consumer/consumer.py

# terminal 3 (after 2 minutes)
python s3/bronze_to_silver.py
python s3/silver_to_gold.py
python s3/load_to_redshift.py
cd stock_pipeline && dbt run && dbt test
```

### 9. Access Airflow

```
http://localhost:8080
username: admin
password: admin
```

---

## Resume Bullets

```
Real-Time Stock Market Analytics Pipeline
Python, Kafka, AWS S3, Redshift Serverless, dbt Core, Airflow, Streamlit

• Ingested live OHLCV data for 50+ NSE tickers via yfinance, streaming
  ~8K JSON events/day through Apache Kafka with schema validation

• Implemented Bronze/Silver/Gold Medallion architecture on AWS S3
  with Parquet storage and Redshift Serverless as the analytical layer

• Built 10+ dbt Core models (RSI, moving averages, volume anomaly)
  with schema tests and full lineage documentation

• Orchestrated end-to-end pipeline using Airflow DAGs with retry
  logic, failure alerting and daily scheduling after NSE market close

• Deployed live interactive dashboard on Streamlit Cloud with
  candlestick charts, RSI signals, sector analysis and pipeline health
```

---

## Known Limitations & Production Improvements

| Current | Production |
|---|---|
| Airflow runs locally on Mac | AWS MWAA or EC2 instance |
| Kafka runs in Docker locally | AWS MSK (Managed Kafka) |
| Single Kafka broker | Multi-broker cluster for HA |
| Manual credential management | AWS Secrets Manager |
| No data quality alerting | Great Expectations + PagerDuty |

---

## Author

**Pratham Jethwani**
- GitHub: [Pratham-Jethwani](https://github.com/Pratham-Jethwani)
- Dashboard: [Live URL](https://your-url.streamlit.app)
