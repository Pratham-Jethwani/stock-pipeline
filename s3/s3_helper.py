import boto3
import os
from dotenv import load_dotenv



import os
ENV_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env')
load_dotenv(ENV_PATH)

def get_s3_client():
    return boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_REGION')
    )

BUCKET = os.getenv('S3_BUCKET_NAME')

# s3 key patterns for each layer
def bronze_key(ticker, date):
    return f"bronze/{date}/{ticker}.json"

def silver_key(ticker, date):
    return f"silver/{date}/{ticker}.parquet"

def gold_key(date):
    return f"gold/{date}/daily_summary.parquet"