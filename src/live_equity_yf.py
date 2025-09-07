# src/live_equity_yf.py
from __future__ import annotations
import time, datetime as dt
from typing import List, Dict
import pandas as pd

def fetch_equity_ohlcv_yahoo(symbols: List[str], period="60d", interval="1h") -> Dict[str, pd.DataFrame]:
    """
    Fetch recent OHLCV from Yahoo via yfinance (delayed, but good for hourly).
    Returns dict: {symbol: DataFrame(Date, Open, High, Low, Close, Volume)}
    """
    try:
        import yfinance as yf
    except Exception as e:
        raise RuntimeError("yfinance not installed; add to requirements.txt") from e

    out = {}
    for sym in symbols:
        try:
            t = yf.Ticker(sym + ".NS")  # Indian symbols on NSE typically .NS
            df = t.history(period=period, interval=interval, auto_adjust=False)
            if df.empty: 
                continue
            df = df.reset_index().rename(columns={"Datetime": "Date"})
            # unify columns
            keep = ["Date","Open","High","Low","Close","Volume"]
            out[sym] = df[keep]
            time.sleep(0.25)  # gentle throttle
        except Exception:
            continue
    return out
