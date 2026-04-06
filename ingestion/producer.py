import json
import time
import logging
from datetime import datetime
import yfinance as yf
from confluent_kafka import Producer
import sys
import os
import pandas as pd
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from kafka_consumer.kafka_config import PRODUCER_CONFIG, TOPIC

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

TICKERS = [
    # IT & technology (10)
    "TCS.NS",        "INFY.NS",       "WIPRO.NS",      "HCLTECH.NS",    "TECHM.NS",
    "LTM.NS",        "MPHASIS.NS",    "PERSISTENT.NS", "COFORGE.NS",    "OFSS.NS",
    # banking (8)
    "HDFCBANK.NS",   "ICICIBANK.NS",  "KOTAKBANK.NS",  "SBIN.NS",       "AXISBANK.NS",
    "INDUSINDBK.NS", "BANDHANBNK.NS", "FEDERALBNK.NS",
    # finance & NBFC (5)
    "BAJFINANCE.NS", "BAJAJFINSV.NS", "HDFCLIFE.NS",   "SBILIFE.NS",    "CHOLAFIN.NS",
    # oil, gas & energy (5)
    "RELIANCE.NS",   "ONGC.NS",       "IOC.NS",        "BPCL.NS",       "POWERGRID.NS",
    # automobile (5)
    "MARUTI.NS",     "TMCV.NS",       "M&M.NS",        "BAJAJ-AUTO.NS", "EICHERMOT.NS",
    # pharma & healthcare (5)
    "SUNPHARMA.NS",  "DRREDDY.NS",    "CIPLA.NS",      "DIVISLAB.NS",   "APOLLOHOSP.NS",
    # FMCG & consumer (5)
    "HINDUNILVR.NS", "ITC.NS",        "NESTLEIND.NS",  "BRITANNIA.NS",  "DABUR.NS",
    # metals & mining (4)
    "TATASTEEL.NS",  "JSWSTEEL.NS",   "HINDALCO.NS",   "COALINDIA.NS",
    # infrastructure & cement (4)
    "ULTRACEMCO.NS", "GRASIM.NS",     "ADANIPORTS.NS", "LT.NS",
    # telecom & media (3)
    "BHARTIARTL.NS", "IDEA.NS",       "ZEEL.NS",
]

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"delivery failed for {msg.key()}: {err}")
    else:
        logger.info(f"delivered to {msg.topic()} partition [{msg.partition()}] offset {msg.offset()}")

def validate_tick(ticker, data):
    if data is None or data.empty:
        logger.warning(f"{ticker}: no data returned")
        return None

    if isinstance(data.columns, pd.MultiIndex):
        data.columns = data.columns.get_level_values(0)

    required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
    for col in required_cols:
        if col not in data.columns:
            logger.warning(f"{ticker}: missing column {col}, skipping")
            return None

    # use second last row to avoid incomplete current candle
    row = data.iloc[-2] if len(data) >= 2 else data.iloc[-1]

    if row[required_cols].isnull().any():
        logger.warning(f"{ticker}: null values found, skipping")
        return None

    if row['Volume'] == 0:
        logger.warning(f"{ticker}: zero volume, skipping")
        return None

    if row['Close'] <= 0:
        logger.warning(f"{ticker}: invalid close price, skipping")
        return None

    return {
        "ticker":      ticker,
        "timestamp":   data.index[-2].isoformat() if len(data) >= 2 else data.index[-1].isoformat(),
        "open_price":        round(float(row['Open']),  4),
        "high_price":        round(float(row['High']),  4),
        "low_price":         round(float(row['Low']),   4),
        "close_price":       round(float(row['Close']), 4),
        "volume":      int(row['Volume']),
        "ingested_at": datetime.utcnow().isoformat() + "Z"
    }

def fetch_and_produce(producer):
    logger.info(f"fetching data for {len(TICKERS)} tickers...")
    success_count = 0
    fail_count    = 0

    for ticker in TICKERS:
        try:
            data = yf.download(
                ticker,
                period="1d",
                interval="1m",
                progress=False,
                auto_adjust=True,
                group_by="ticker"
            )

            if isinstance(data.columns, pd.MultiIndex):
                data = data[ticker] if ticker in data.columns.get_level_values(0) else data
                data.columns = data.columns.get_level_values(0) if isinstance(data.columns, pd.MultiIndex) else data.columns

            event = validate_tick(ticker, data)
            if event is None:
                fail_count += 1
                continue

            producer.produce(
                topic=TOPIC,
                key=ticker,
                value=json.dumps(event),
                callback=delivery_report
            )
            success_count += 1

        except Exception as e:
            logger.error(f"{ticker}: unexpected error — {e}")
            fail_count += 1

    producer.flush()
    logger.info(f"batch complete — success: {success_count}, skipped: {fail_count}")

def main():
    logger.info("starting stock pipeline producer...")
    producer = Producer(PRODUCER_CONFIG)

    try:
        while True:
            import pytz
            ist = pytz.timezone('Asia/Kolkata')
            now_ist = datetime.now(ist)

            is_weekday      = now_ist.weekday() < 5
            market_open     = now_ist.replace(hour=9,  minute=15, second=0, microsecond=0)
            market_close    = now_ist.replace(hour=15, minute=30, second=0, microsecond=0)
            is_market_hours = market_open <= now_ist <= market_close

            if is_weekday and is_market_hours:
                logger.info(f"market open — IST {now_ist.strftime('%H:%M:%S')}")
                fetch_and_produce(producer)
                logger.info("sleeping 60 seconds...")
                time.sleep(60)
            else:
                logger.info(f"market closed — IST {now_ist.strftime('%H:%M:%S')} — sleeping 5 minutes...")
                time.sleep(300)

    except KeyboardInterrupt:
        logger.info("producer stopped by user")
    finally:
        producer.flush()

if __name__ == "__main__":
    main()