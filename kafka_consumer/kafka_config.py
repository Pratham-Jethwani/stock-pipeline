import os
from dotenv import load_dotenv

load_dotenv()

PRODUCER_CONFIG = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
    'client.id': 'stock-producer',
    'acks': 'all',
    'retries': 3,
    'retry.backoff.ms': 500,
}

CONSUMER_CONFIG = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
    'group.id': 'stock-consumer-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
}

TOPIC = os.getenv('KAFKA_TOPIC', 'stock_ticks')