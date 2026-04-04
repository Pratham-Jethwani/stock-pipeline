import redshift_connector
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

CREATE_STAGING = """
CREATE TABLE IF NOT EXISTS staging_stock_ticks (
    trade_date     VARCHAR(10),
    ticker         VARCHAR(20),
    open_price     FLOAT,
    high_price     FLOAT,
    low_price      FLOAT,
    close_price    FLOAT,
    volume         BIGINT,
    ma_7           FLOAT,
    ma_20          FLOAT,
    rsi_14         FLOAT,
    daily_return   FLOAT,
    volume_anomaly BOOLEAN,
    loaded_at      TIMESTAMP DEFAULT GETDATE()
);
"""

CREATE_COMPANIES = """
CREATE TABLE IF NOT EXISTS dim_companies (
    ticker        VARCHAR(20) PRIMARY KEY,
    company_name  VARCHAR(100),
    sector        VARCHAR(50),
    stock_exchange VARCHAR(10) DEFAULT 'NSE'
);
"""

def main():
    print("connecting to redshift...")
    conn   = get_connection()
    cursor = conn.cursor()

    print("creating staging_stock_ticks...")
    cursor.execute(CREATE_STAGING)

    print("creating dim_companies...")
    cursor.execute(CREATE_COMPANIES)

    conn.commit()
    conn.close()
    print("all tables created successfully")

if __name__ == "__main__":
    main()