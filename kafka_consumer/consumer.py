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

    # read existing file if it exists, append new messages
    existing = []
    try:
        obj = s3_client.get_object(Bucket=BUCKET, Key=key)
        existing = json.loads(obj['Body'].read().decode('utf-8'))
    except s3_client.exceptions.NoSuchKey:
        pass
    except Exception:
        pass

    existing.extend(messages)

    s3_client.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(existing, indent=2).encode('utf-8'),
        ContentType='application/json'
    )
    logger.info(f"bronze: wrote {len(messages)} events for {ticker} to s3://{BUCKET}/{key}")

def main():
    logger.info("starting kafka consumer → bronze layer...")
    consumer  = Consumer(CONSUMER_CONFIG)
    s3_client = get_s3_client()

    consumer.subscribe([TOPIC])

    # buffer: { ticker: [list of events] }
    buffer = defaultdict(list)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # flush any remaining buffered messages
                for ticker, messages in buffer.items():
                    if messages:
                        write_to_bronze(s3_client, ticker, messages)
                buffer.clear()
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"consumer error: {msg.error()}")
                    continue

            # parse the message
            event  = json.loads(msg.value().decode('utf-8'))
            ticker = event['ticker']
            buffer[ticker].append(event)

            # write to s3 when batch is full
            if len(buffer[ticker]) >= BATCH_SIZE:
                write_to_bronze(s3_client, ticker, buffer[ticker])
                buffer[ticker] = []

            # manually commit offset after successful s3 write
            consumer.commit(msg)

    except KeyboardInterrupt:
        logger.info("consumer stopped — flushing remaining buffer...")
        for ticker, messages in buffer.items():
            if messages:
                write_to_bronze(s3_client, ticker, messages)
        logger.info("buffer flushed")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()