# NSE Stock Market Analytics Pipeline

An end-to-end real-time data pipeline that ingests live OHLCV data for 50+ NSE tickers, processes it through a Medallion architecture on AWS S3, transforms it with dbt Core, orchestrates daily runs via Apache Airflow, and visualizes insights on a live Streamlit dashboard.

**Live Dashboard:** [stock-pipeline.streamlit.app](https://stock-pipeline-1910.streamlit.app/)

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

| Layer          | Technology                      |
| -------------- | ------------------------------- |
| Data Source    | yfinance (Yahoo Finance API)    |
| Message Queue  | Apache Kafka (Docker)           |
| Data Lake      | AWS S3 (Medallion Architecture) |
| Data Warehouse | AWS Redshift Serverless         |
| Transformation | dbt Core                        |
| Orchestration  | Apache Airflow (Docker)         |
| Dashboard      | Streamlit Cloud                 |
| Language       | Python 3.11                     |
| Infrastructure | Docker, Docker Compose          |

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

| Sector          | Tickers                                                                                       |
| --------------- | --------------------------------------------------------------------------------------------- |
| IT & Technology | TCS, Infosys, Wipro, HCL Tech, Tech Mahindra, LTIMindtree, Mphasis, Persistent, Coforge, OFSS |
| Banking         | HDFC Bank, ICICI Bank, Kotak Bank, SBI, Axis Bank, IndusInd, Bandhan, Federal Bank            |
| Finance & NBFC  | Bajaj Finance, Bajaj Finserv, HDFC Life, SBI Life, Cholamandalam                              |
| Oil & Energy    | Reliance, ONGC, IOC, BPCL, Power Grid                                                         |
| Automobile      | Maruti, Tata Motors CV, M&M, Bajaj Auto, Eicher Motors                                        |
| Pharma          | Sun Pharma, Dr Reddy's, Cipla, Divi's Lab, Apollo Hospitals                                   |
| FMCG            | HUL, ITC, Nestle, Britannia, Dabur                                                            |
| Metals & Mining | Tata Steel, JSW Steel, Hindalco, Coal India                                                   |
| Infrastructure  | UltraTech, Grasim, Adani Ports, L&T                                                           |
| Telecom & Media | Bharti Airtel, Vodafone Idea, Zee Entertainment                                               |

### Data Collected per Ticker

```json
{
  "ticker": "TCS.NS",
  "timestamp": "2026-04-06T09:16:00+05:30",
  "open_price": 3812.5,
  "high_price": 3819.0,
  "low_price": 3810.0,
  "close_price": 3815.75,
  "volume": 14250,
  "ingested_at": "2026-04-06T03:46:05.123Z"
}
```

### Medallion Architecture

| Layer  | Format  | Content                                       |
| ------ | ------- | --------------------------------------------- |
| Bronze | JSON    | Raw tick events, partitioned by `date/ticker` |
| Silver | Parquet | Cleaned, typed, deduplicated 1-min candles    |
| Gold   | Parquet | Daily OHLCV summary + computed indicators     |

### Computed Indicators

| Indicator      | Description                                |
| -------------- | ------------------------------------------ |
| MA 7           | 7-period moving average of close price     |
| MA 20          | 20-period moving average of close price    |
| RSI 14         | 14-period Relative Strength Index          |
| Daily Return   | `(close - open) / open × 100`              |
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

| Page             | Description                                      |
| ---------------- | ------------------------------------------------ |
| Market Overview  | Gainers, losers, sector performance heatmap      |
| Stock Analysis   | Candlestick chart with MA7/MA20 overlay + volume |
| RSI Signals      | RSI bar chart with overbought/oversold zones     |
| Volume Anomalies | Tickers with unusual trading activity            |
| Pipeline Health  | Row counts, data freshness, sector distribution  |

---

## Known Limitations & Production Improvements

| Current                      | Production                     |
| ---------------------------- | ------------------------------ |
| Airflow runs locally on Mac  | AWS MWAA or EC2 instance       |
| Kafka runs in Docker locally | AWS MSK (Managed Kafka)        |
| Single Kafka broker          | Multi-broker cluster for HA    |
| Manual credential management | AWS Secrets Manager            |
| No data quality alerting     | Great Expectations + PagerDuty |

---

## Author

**Pratham Jethwani**

- Dashboard: [Live URL](https://stock-pipeline-1910.streamlit.app/)
