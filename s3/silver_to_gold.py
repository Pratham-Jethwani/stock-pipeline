import logging
import os
import sys
from datetime import date
from io import BytesIO

import pandas as pd
import boto3
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from s3.s3_helper import get_s3_client, BUCKET, silver_key, gold_key

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

def compute_rsi(series, period=14):
    delta  = series.diff()
    gain   = delta.clip(lower=0)
    loss   = -delta.clip(upper=0)
    avg_gain = gain.rolling(window=period, min_periods=1).mean()
    avg_loss = loss.rolling(window=period, min_periods=1).mean()
    rs  = avg_gain / avg_loss.replace(0, 1e-10)
    rsi = 100 - (100 / (1 + rs))
    return round(rsi.iloc[-1], 2)

def main():
    today     = date.today().isoformat()
    s3_client = get_s3_client()

    logger.info(f"starting silver → gold for {today}...")

    all_summaries = []

    for ticker in TICKERS:
        s_key = silver_key(ticker, today)
        try:
            obj    = s3_client.get_object(Bucket=BUCKET, Key=s_key)
            df     = pd.read_parquet(BytesIO(obj['Body'].read()))

            if df.empty:
                continue

            # daily summary per ticker
            summary = {
                "trade_date":           today,
                "ticker":         ticker,
                "open_price":           round(float(df['open_price'].iloc[0]),   2),
                "high_price":           round(float(df['high_price'].max()),      2),
                "low_price":            round(float(df['low_price'].min()),       2),
                "close_price":          round(float(df['close_price'].iloc[-1]), 2),
                "volume":         int(df['volume'].sum()),
                "ma_7":           round(float(df['close_price'].rolling(7,  min_periods=1).mean().iloc[-1]), 2),
                "ma_20":          round(float(df['close_price'].rolling(20, min_periods=1).mean().iloc[-1]), 2),
                "rsi_14":         compute_rsi(df['close_price']),
                "daily_return":   round(float((df['close_price'].iloc[-1] - df['open_price'].iloc[0]) / df['open_price'].iloc[0] * 100), 4),
                "volume_anomaly": bool(df['volume'].sum() > df['volume'].mean() * 2),
            }
            all_summaries.append(summary)
            logger.info(f"gold: computed summary for {ticker}")

        except Exception as e:
            logger.warning(f"{ticker}: skipped — {e}")

    if not all_summaries:
        logger.warning("no summaries to write — exiting")
        return

    # write combined gold parquet
    gold_df = pd.DataFrame(all_summaries)
    g_key   = gold_key(today)
    buffer  = BytesIO()
    gold_df.to_parquet(buffer, index=False, engine='pyarrow')
    buffer.seek(0)

    s3_client.put_object(
        Bucket=BUCKET,
        Key=g_key,
        Body=buffer.getvalue(),
        ContentType='application/octet-stream'
    )
    logger.info(f"gold: wrote {len(gold_df)} rows → s3://{BUCKET}/{g_key}")

if __name__ == "__main__":
    main()