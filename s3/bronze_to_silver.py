import json
import logging
import os
import sys
from datetime import date
from io import BytesIO

import pandas as pd
import boto3
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from s3.s3_helper import get_s3_client, BUCKET, bronze_key, silver_key

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

TICKERS = [
    "TCS.NS",       "INFY.NS",      "WIPRO.NS",     "HCLTECH.NS",   "TECHM.NS",
    "LTM.NS",       "MPHASIS.NS",   "PERSISTENT.NS","COFORGE.NS",   "OFSS.NS",
    "HDFCBANK.NS",  "ICICIBANK.NS", "KOTAKBANK.NS", "SBIN.NS",      "AXISBANK.NS",
    "INDUSINDBK.NS","BANDHANBNK.NS","FEDERALBNK.NS",
    "BAJFINANCE.NS","BAJAJFINSV.NS","HDFCLIFE.NS",  "SBILIFE.NS",   "CHOLAFIN.NS",
    "RELIANCE.NS",  "ONGC.NS",      "IOC.NS",       "BPCL.NS",      "POWERGRID.NS",
    "MARUTI.NS",    "TMCV.NS",      "M&M.NS",       "BAJAJ-AUTO.NS","EICHERMOT.NS",
    "SUNPHARMA.NS", "DRREDDY.NS",   "CIPLA.NS",     "DIVISLAB.NS",  "APOLLOHOSP.NS",
    "HINDUNILVR.NS","ITC.NS",       "NESTLEIND.NS", "BRITANNIA.NS", "DABUR.NS",
    "TATASTEEL.NS", "JSWSTEEL.NS",  "HINDALCO.NS",  "COALINDIA.NS",
    "ULTRACEMCO.NS","GRASIM.NS",    "ADANIPORTS.NS","LT.NS",
    "BHARTIARTL.NS","IDEA.NS",      "ZEEL.NS",
]

def process_ticker(s3_client, ticker, process_date):
    b_key = bronze_key(ticker, process_date)

    # read bronze json
    try:
        obj  = s3_client.get_object(Bucket=BUCKET, Key=b_key)
        data = json.loads(obj['Body'].read().decode('utf-8'))
    except Exception as e:
        logger.warning(f"{ticker}: no bronze data found for {process_date} — {e}")
        return False

    if not data:
        logger.warning(f"{ticker}: empty bronze file, skipping")
        return False

    # convert to dataframe
    df = pd.DataFrame(data)

    # cast types
    df['timestamp']   = pd.to_datetime(df['timestamp'])
    df['ingested_at'] = pd.to_datetime(df['ingested_at'])
    df['open_price']        = df['open_price'].astype(float)
    df['high_price']        = df['high_price'].astype(float)
    df['low_price']         = df['low_price'].astype(float)
    df['close_price']       = df['close_price'].astype(float)
    df['volume']      = df['volume'].astype(int)

    # clean: drop nulls and duplicates
    df = df.dropna(subset=['open_price', 'high_price', 'low_price', 'close_price', 'volume'])
    df = df.drop_duplicates(subset=['ticker', 'timestamp'])

    # sort by timestamp
    df = df.sort_values('timestamp').reset_index(drop=True)

    # write as parquet to silver
    s_key  = silver_key(ticker, process_date)
    buffer = BytesIO()
    df.to_parquet(buffer, index=False, engine='pyarrow')
    buffer.seek(0)

    s3_client.put_object(
        Bucket=BUCKET,
        Key=s_key,
        Body=buffer.getvalue(),
        ContentType='application/octet-stream'
    )
    logger.info(f"silver: wrote {len(df)} rows for {ticker} → s3://{BUCKET}/{s_key}")
    return True

def main():
    today     = date.today().isoformat()
    s3_client = get_s3_client()

    logger.info(f"starting bronze → silver for {today}...")
    success = fail = 0

    for ticker in TICKERS:
        result = process_ticker(s3_client, ticker, today)
        if result:
            success += 1
        else:
            fail += 1

    logger.info(f"bronze → silver complete — success: {success}, skipped: {fail}")

if __name__ == "__main__":
    main()