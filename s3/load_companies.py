import redshift_connector
import yfinance as yf
from dotenv import dotenv_values
import time

def get_connection():
    v = dotenv_values('/Users/pratham/Desktop/stock-pipeline/.env')
    return redshift_connector.connect(
        host=v.get('REDSHIFT_HOST'),
        database=v.get('REDSHIFT_DB'),
        port=int(v.get('REDSHIFT_PORT')),
        user=v.get('REDSHIFT_USER'),
        password=v.get('REDSHIFT_PASSWORD')
    )

TICKERS = [
    "TCS.NS",        "INFY.NS",       "WIPRO.NS",      "HCLTECH.NS",    "TECHM.NS",
    "LTM.NS",        "MPHASIS.NS",    "PERSISTENT.NS", "COFORGE.NS",    "OFSS.NS",
    "HDFCBANK.NS",   "ICICIBANK.NS",  "KOTAKBANK.NS",  "SBIN.NS",       "AXISBANK.NS",
    "INDUSINDBK.NS", "BANDHANBNK.NS", "FEDERALBNK.NS",
    "BAJFINANCE.NS", "BAJAJFINSV.NS", "HDFCLIFE.NS",   "SBILIFE.NS",    "CHOLAFIN.NS",
    "RELIANCE.NS",   "ONGC.NS",       "IOC.NS",        "BPCL.NS",       "POWERGRID.NS",
    "MARUTI.NS",     "TMCV.NS",       "M&M.NS",        "BAJAJ-AUTO.NS", "EICHERMOT.NS",
    "SUNPHARMA.NS",  "DRREDDY.NS",    "CIPLA.NS",      "DIVISLAB.NS",   "APOLLOHOSP.NS",
    "HINDUNILVR.NS", "ITC.NS",        "NESTLEIND.NS",  "BRITANNIA.NS",  "DABUR.NS",
    "TATASTEEL.NS",  "JSWSTEEL.NS",   "HINDALCO.NS",   "COALINDIA.NS",
    "ULTRACEMCO.NS", "GRASIM.NS",     "ADANIPORTS.NS", "LT.NS",
    "BHARTIARTL.NS", "IDEA.NS",       "ZEEL.NS",
]

def fetch_company_info(ticker):
    try:
        info = yf.Ticker(ticker).info
        return {
            "ticker":       ticker,
            "company_name": info.get("longName")     or info.get("shortName") or ticker,
            "sector":       info.get("sector")       or "Unknown",
            "industry":     info.get("industry")     or "Unknown",
            "market_cap":   info.get("marketCap")    or 0,
            "exchange":     info.get("exchange")     or "NSE",
        }
    except Exception as e:
        print(f"  WARN: could not fetch info for {ticker} — {e}")
        return {
            "ticker":       ticker,
            "company_name": ticker,
            "sector":       "Unknown",
            "industry":     "Unknown",
            "market_cap":   0,
            "exchange":     "NSE",
        }

def main():
    # first update dim_companies table to include new columns
    conn   = get_connection()
    cursor = conn.cursor()

    print("recreating dim_companies with full metadata...")
    cursor.execute("DROP TABLE IF EXISTS dim_companies")
    cursor.execute("""
        CREATE TABLE dim_companies (
            ticker        VARCHAR(20) PRIMARY KEY,
            company_name  VARCHAR(200),
            sector        VARCHAR(100),
            industry      VARCHAR(100),
            market_cap    BIGINT,
            stock_exchange VARCHAR(20)
        )
    """)
    conn.commit()

    print(f"fetching metadata for {len(TICKERS)} tickers from yfinance...")
    companies = []

    for i, ticker in enumerate(TICKERS):
        print(f"  [{i+1}/{len(TICKERS)}] fetching {ticker}...")
        info = fetch_company_info(ticker)
        companies.append(info)
        time.sleep(0.5)  # be polite to yahoo finance api

    print(f"\ninserting {len(companies)} rows into dim_companies...")
    for c in companies:
        cursor.execute(
            """
            INSERT INTO dim_companies
                (ticker, company_name, sector, industry, market_cap, stock_exchange)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                c['ticker'],
                c['company_name'],
                c['sector'],
                c['industry'],
                c['market_cap'],
                c['exchange'],
            )
        )

    conn.commit()

    cursor.execute("SELECT COUNT(*) FROM dim_companies")
    count = cursor.fetchone()[0]
    print(f"\ndim_companies populated with {count} rows")

    # preview
    cursor.execute("SELECT ticker, company_name, sector, market_cap FROM dim_companies LIMIT 5")
    rows = cursor.fetchall()
    print("\npreview:")
    print(f"{'ticker':<20} {'company_name':<35} {'sector':<30} {'market_cap'}")
    print("-" * 100)
    for row in rows:
        print(f"{row[0]:<20} {row[1]:<35} {row[2]:<30} {row[3]:,}")

    conn.close()

if __name__ == "__main__":
    main()