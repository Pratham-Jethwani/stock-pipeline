import yfinance as yf
import pandas as pd

TICKERS = [
    # IT & technology (10)
    "TCS.NS",       "INFY.NS",      "WIPRO.NS",     "HCLTECH.NS",   "TECHM.NS",
    "LTM.NS",      "MPHASIS.NS",   "PERSISTENT.NS","COFORGE.NS",   "OFSS.NS",

    # banking (8)
    "HDFCBANK.NS",  "ICICIBANK.NS", "KOTAKBANK.NS", "SBIN.NS",      "AXISBANK.NS",
    "INDUSINDBK.NS","BANDHANBNK.NS","FEDERALBNK.NS",

    # finance & NBFC (5)
    "BAJFINANCE.NS","BAJAJFINSV.NS","HDFCLIFE.NS",  "SBILIFE.NS",   "CHOLAFIN.NS",

    # oil, gas & energy (5)
    "RELIANCE.NS",  "ONGC.NS",      "IOC.NS",       "BPCL.NS",      "POWERGRID.NS",

    # automobile (5)
    "MARUTI.NS",    "TMCV.NS","M&M.NS",       "BAJAJ-AUTO.NS","EICHERMOT.NS",

    # pharma & healthcare (5)
    "SUNPHARMA.NS", "DRREDDY.NS",   "CIPLA.NS",     "DIVISLAB.NS",  "APOLLOHOSP.NS",

    # FMCG & consumer (5)
    "HINDUNILVR.NS","ITC.NS",       "NESTLEIND.NS", "BRITANNIA.NS", "DABUR.NS",

    # metals & mining (4)
    "TATASTEEL.NS", "JSWSTEEL.NS",  "HINDALCO.NS",  "COALINDIA.NS",

    # infrastructure & cement (4)
    "ULTRACEMCO.NS","GRASIM.NS",    "ADANIPORTS.NS","LT.NS",

    # telecom & media (3)
    "BHARTIARTL.NS","IDEA.NS",      "ZEEL.NS",
]

print(f"validating {len(TICKERS)} tickers...\n")

valid   = []
invalid = []

for ticker in TICKERS:
    try:
        data = yf.download(
            ticker,
            period="5d",
            interval="1d",
            progress=False,
            auto_adjust=True
        )
        if data.empty:
            invalid.append(ticker)
            print(f"  FAIL   {ticker} — no data returned")
        else:
            valid.append(ticker)
            print(f"  OK     {ticker} — {len(data)} rows, latest close: {round(float(data['Close'].iloc[-1]), 2)}")
    except Exception as e:
        invalid.append(ticker)
        print(f"  ERROR  {ticker} — {e}")

print(f"\n--- summary ---")
print(f"valid:   {len(valid)}")
print(f"invalid: {len(invalid)}")

if invalid:
    print(f"\nremove these from TICKERS:")
    for t in invalid:
        print(f"  {t}")