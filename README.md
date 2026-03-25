# Stock Market Analytics Pipeline

End-to-end data pipeline ingesting live OHLCV data for 20 tickers via yfinance,
streaming through Apache Kafka, stored in AWS S3 (Medallion architecture),
transformed with dbt Core, orchestrated via Airflow, and visualized in Streamlit.

## Tech Stack
- Python, Apache Kafka, AWS S3, AWS Redshift Serverless, dbt Core, Apache Airflow, Streamlit

## Setup
1. Clone the repo
2. Run `python3 -m venv venv && source venv/bin/activate`
3. Run `pip install -r requirements.txt`
4. Copy `.env.example` to `.env` and fill in your credentials