import redshift_connector
import os
from datetime import date
from dotenv import dotenv_values

def get_connection():
    v = dotenv_values('/Users/pratham/Desktop/stock-pipeline/.env')
    return redshift_connector.connect(
        host=v.get('REDSHIFT_HOST'),
        database=v.get('REDSHIFT_DB'),
        port=int(v.get('REDSHIFT_PORT')),
        user=v.get('REDSHIFT_USER'),
        password=v.get('REDSHIFT_PASSWORD')
    )

def main():
    today    = date.today().isoformat()
    v        = dotenv_values('/Users/pratham/Desktop/stock-pipeline/.env')
    bucket   = v.get('S3_BUCKET_NAME')
    role_arn = v.get('REDSHIFT_IAM_ROLE_ARN')
    gold_path = f"s3://{bucket}/gold/{today}/daily_summary.parquet"

    conn   = get_connection()
    cursor = conn.cursor()

    # delete today's data if already loaded (idempotent)
    print(f"removing existing data for {today}...")
    cursor.execute(
        "DELETE FROM staging_stock_ticks WHERE trade_date = %s",
        (today,)
    )

    # load from s3 using copy command
    print(f"loading gold data from {gold_path}...")
    copy_sql = f"""
        COPY staging_stock_ticks (
            trade_date, ticker,
            open_price, high_price, low_price, close_price,
            volume, ma_7, ma_20, rsi_14,
            daily_return, volume_anomaly
        )
        FROM '{gold_path}'
        IAM_ROLE '{role_arn}'
        FORMAT AS PARQUET;
    """
    cursor.execute(copy_sql)
    conn.commit()

    # verify row count
    cursor.execute(
        "SELECT COUNT(*) FROM staging_stock_ticks WHERE trade_date = %s",
        (today,)
    )
    count = cursor.fetchone()[0]
    print(f"loaded {count} rows for {today}")
    conn.close()

if __name__ == "__main__":
    main()