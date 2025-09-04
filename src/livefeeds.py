# src/livefeeds.py
"""
Free live equity data (yfinance) with simple persistence into datalake.
- Pulls last N days of OHLCV for your universe
- Writes datalake/daily_equity.parquet and per_symbol/*.csv
- Returns a dict with source + counts so pipeline can log it
"""

from __future__ import annotations
import os, math
from typing import Iterable, Dict
import pandas as pd
import datetime as dt

try:
    import yfinance as yf
except Exception:
    yf = None

DL_ROOT = "datalake"

def _ensure_dirs():
    os.makedirs(DL_ROOT, exist_ok=True)
    os.makedirs(os.path.join(DL_ROOT, "per_symbol"), exist_ok=True)

def _load_universe_from_datalake() -> list[str]:
    # try explicit universe.csv first
    uni = os.path.join(DL_ROOT, "universe.csv")
    if os.path.exists(uni):
        s = pd.read_csv(uni)
        c = "Symbol" if "Symbol" in s.columns else s.columns[0]
        syms = [str(x).strip() for x in s[c].dropna().tolist()]
        return [x for x in syms if x]

    # else infer from existing per_symbol files
    ps = os.path.join(DL_ROOT, "per_symbol")
    if os.path.isdir(ps):
        return [f.replace(".csv","") for f in os.listdir(ps) if f.endswith(".csv")]

    # fallback tiny set if nothing present
    return ["INFY.NS", "TCS.NS", "RELIANCE.NS", "HDFCBANK.NS", "ICICIBANK.NS"]

def _normalize(sym: str) -> str:
    s = str(sym).strip()
    # add .NS if no exchange suffix present (best effort for India)
    if "." not in s:
        s = s + ".NS"
    return s

def _fetch_one(sym: str, days: int = 60, interval: str = "1d") -> pd.DataFrame:
    if yf is None:
        return pd.DataFrame()
    t = yf.Ticker(sym)
    # yfinance: using period keeps it simple and robust
    try:
        hist = t.history(period=f"{max(1, days)}d", interval=interval, auto_adjust=False, prepost=False)
        if hist is None or hist.empty:
            return pd.DataFrame()
        hist = hist.reset_index().rename(columns={
            "Date": "Date", "Open": "Open", "High": "High",
            "Low": "Low", "Close": "Close", "Volume":"Volume"
        })
        hist["Symbol"] = sym
        return hist[["Date","Symbol","Open","High","Low","Close","Volume"]]
    except Exception:
        return pd.DataFrame()

def refresh_equity_data(symbols: Iterable[str] | None = None, days: int = 60, interval: str = "1d") -> Dict:
    """
    Pulls equity OHLCV via yfinance and persists to datalake.
    Returns: {"equities_source":"yfinance"|"none", "rows": int, "symbols": int}
    """
    _ensure_dirs()
    if symbols is None:
        symbols = _load_universe_from_datalake()

    symbols = [_normalize(s) for s in symbols]
    frames = []
    for s in symbols:
        df = _fetch_one(s, days=days, interval=interval)
        if not df.empty:
            frames.append(df)
            # also write per_symbol
            df.to_csv(os.path.join(DL_ROOT, "per_symbol", f"{s}.csv"), index=False)

    if frames:
        full = pd.concat(frames, ignore_index=True)
        # coerce dtypes
        full["Date"] = pd.to_datetime(full["Date"])
        # write parquet and csv snapshot
        try:
            full.to_parquet(os.path.join(DL_ROOT, "daily_equity.parquet"), index=False)
        except Exception:
            pass
        full.to_csv(os.path.join(DL_ROOT, "daily_equity.csv"), index=False)
        return {"equities_source":"yfinance", "rows": int(len(full)), "symbols": int(full["Symbol"].nunique())}
    else:
        return {"equities_source":"none", "rows": 0, "symbols": 0}
