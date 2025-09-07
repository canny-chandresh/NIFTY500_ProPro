"""
Alternate equity data fetcher (5m/15m intraday) with Yahoo fallback.
Use in pipeline to upgrade intraday learning.
"""

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

def fetch_intraday(symbol: str, interval="5m", lookback_days=5) -> pd.DataFrame:
    """
    Fetch intraday candles. Falls back gracefully.
    """
    try:
        df = yf.download(symbol, interval=interval, period=f"{lookback_days}d", progress=False)
        df = df.reset_index()
        df.rename(columns={"Datetime":"Date"}, inplace=True)
        return df
    except Exception as e:
        print(f"[live_equity_alt] Fallback: {e}")
        return pd.DataFrame()

if __name__ == "__main__":
    print(fetch_intraday("RELIANCE.NS").head())
