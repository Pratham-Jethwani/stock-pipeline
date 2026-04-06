import json
import logging
import os
import sys
from datetime import datetime, date
from collections import defaultdict

import boto3
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from kafka_consumer.kafka_config import CONSUMER_CONFIG, TOPIC
from s3.s3_helper import get_s3_client, BUCKET, bronze_key

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# batch messages per ticker before writing to s3
BATCH_SIZE = 10

def write_to_bronze(s3_client, ticker, messages):
    today = date.today().isoformat()
    key   = bronze_key(ticker, today)

    # read existing file if it exists
    existing = []
    try:
        obj      = s3_client.get_object(Bucket=BUCKET, Key=key)
        existing = json.loads(obj['Body'].read().decode('utf-8'))
    except Exception:
        pass

    # deduplicate by timestamp before writing
    existing.extend(messages)
    seen       = set()
    deduped    = []
    for event in existing:
        ts = event.get('timestamp')
        tk = event.get('ticker')
        key_str = f"{tk}_{ts}"
        if key_str not in seen:
            seen.add(key_str)
            deduped.append(event)

    s3_client.put_object(
        Bucket=BUCKET,
        Key=bronze_key(ticker, today),
        Body=json.dumps(deduped, indent=2).encode('utf-8'),
        ContentType='application/json'
    )
    logger.info(f"bronze: wrote {len(messages)} events for {ticker} — total {len(deduped)} unique events today")

    
def main():
    logger.info("starting kafka consumer → bronze layer...")
    consumer  = Consumer(CONSUMER_CONFIG)
    s3_client = get_s3_client()

    consumer.subscribe([TOPIC])

    buffer = defaultdict(list)

    # run for fixed time then exit — works for both manual and airflow runs
    import time
    run_duration = 120  # run for 2 minutes
    start_time   = time.time()

    try:
        while time.time() - start_time < run_duration:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"consumer error: {msg.error()}")
                    continue

            raw = msg.value()
            if not raw:
                consumer.commit(msg)
                continue

            try:
                event = json.loads(raw.decode('utf-8'))
            except json.JSONDecodeError as e:
                logger.warning(f"skipping malformed message: {e}")
                consumer.commit(msg)
                continue

            ticker = event.get('ticker')
            if not ticker:
                logger.warning("skipping message with no ticker field")
                consumer.commit(msg)
                continue

            buffer[ticker].append(event)

            if len(buffer[ticker]) >= BATCH_SIZE:
                write_to_bronze(s3_client, ticker, buffer[ticker])
                buffer[ticker] = []

            consumer.commit(msg)

        # flush remaining buffer
        logger.info("time limit reached — flushing remaining buffer to s3...")
        for ticker, messages in buffer.items():
            if messages:
                write_to_bronze(s3_client, ticker, messages)

        logger.info("consumer finished successfully")

    except KeyboardInterrupt:
        logger.info("consumer stopped — flushing remaining buffer...")
        for ticker, messages in buffer.items():
            if messages:
                write_to_bronze(s3_client, ticker, messages)
    finally:
        consumer.close()

if __name__ == "__main__":
    main()