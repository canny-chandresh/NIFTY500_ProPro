# -*- coding: utf-8 -*-
"""
nse_client.py — lightweight NSE fetcher with session/cookie bootstrap and retries.
Endpoints may change; this is best-effort with polite headers and backoff.
Falls back to caller if 403/empty.
"""
from __future__ import annotations
import time, json, random
from pathlib import Path
from typing import Optional, Dict, Any, List
import pandas as pd
import requests

from config import CONFIG

BASE = "https://www.nseindia.com"
HDRS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/119.0 Safari/537.36",
    "Accept": "application/json,text/plain,*/*",
    "Referer": "https://www.nseindia.com/",
    "Connection": "keep-alive",
}

def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HDRS)
    # warm-up to get cookies
    try:
        s.get(BASE, timeout=CONFIG["data_sources"]["nse"]["timeout_sec"])
    except Exception:
        pass
    return s

def _get_json(s: requests.Session, url: str, params: Optional[Dict[str,Any]]=None, retries: int=1) -> Optional[Dict[str,Any]]:
    for i in range(retries+1):
        try:
            r = s.get(url, params=params, timeout=CONFIG["data_sources"]["nse"]["timeout_sec"])
            if r.status_code == 200 and r.headers.get("Content-Type","").startswith("application/json"):
                return r.json()
            # small delay/backoff
            time.sleep(0.8 + 0.4*random.random())
        except Exception:
            time.sleep(0.8)
    return None

def daily_equity(symbol: str) -> pd.DataFrame:
    """
    Attempts to fetch recent daily candles from NSE 'chart-databyindex' API for equity.
    If JSON parsing fails, returns empty DF (caller should fallback).
    """
    s = _session()
    # symbol must be plain (no .NS)
    sym = symbol.replace(".NS","").upper()
    # historical daily via chart endpoint (index=equity)
    # Known endpoint (subject to change):
    url = f"{BASE}/api/chart-databyindex?index={sym}%20EQ&indices=true"
    js = _get_json(s, url, retries=CONFIG["data_sources"]["nse"]["retries"])
    if not js or "grapthData" not in js:
        return pd.DataFrame()
    # grapthData: [[ts, close], ...] — we’ll also fetch quote to align OHLC if needed
    rows = js.get("grapthData", [])
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["ts", "close"])
    df["date"] = pd.to_datetime(df["ts"], unit="ms").dt.date
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["symbol"] = sym
    # NSE chart gives close; we’ll approximate OHLC with quote endpoint (optional)
    # quote: /api/quote-equity?symbol=RELIANCE
    q = _get_json(s, f"{BASE}/api/quote-equity", params={"symbol": sym}, retries=1)
    if q and "priceInfo" in q:
        last = q["priceInfo"]
        # Use last known LTP as latest close if newer
        if pd.notnull(last.get("lastPrice")) and len(df):
            df.loc[df.index[-1], "close"] = float(last["lastPrice"])
    # We’ll keep only close; pipeline will compute indicators; OHLC filled by fallback if needed
    return df[["date","close","symbol"]]

def intraday_5m_today(symbol: str) -> pd.DataFrame:
    """
    Attempts NSE intraday OHLC for today from 'chart-databyindex' (5m) if possible.
    Returns empty DF if blocked; caller should fallback.
    """
    s = _session()
    sym = symbol.replace(".NS","").upper()
    # /api/chart-databyindex?index=RELIANCE%20EQ&indices=true — usually provides intraday arrays `grapthData` 1m/5m mixed
    url = f"{BASE}/api/chart-databyindex?index={sym}%20EQ&indices=true"
    js = _get_json(s, url, retries=CONFIG["data_sources"]["nse"]["retries"])
    if not js or "grapthData" not in js:
        return pd.DataFrame()
    rows = js.get("grapthData", [])
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["ts","price"])
    df["datetime"] = pd.to_datetime(df["ts"], unit="ms")
    df["close"] = pd.to_numeric(df["price"], errors="coerce")
    df["open"]  = df["close"]  # best-effort; will be refined via resample
    df["high"]  = df["close"]
    df["low"]   = df["close"]
    df["volume"]= 0.0
    # resample to 5m OHLC (best-effort)
    g = df.set_index("datetime").resample("5min").agg(
        open=("open","first"),
        high=("high","max"),
        low=("low","min"),
        close=("close","last"),
        volume=("volume","sum")
    ).dropna(subset=["close"])
    g = g.reset_index()
    # keep only today
    if len(g):
        g = g[g["datetime"].dt.date == g["datetime"].dt.date.max()]
    return g

def options_chain(symbol: str, is_index: bool=True) -> Dict[str,Any]:
    """
    Fetch options chain JSON for index or equity.
    """
    s = _session()
    sym = symbol.replace(".NS","").upper()
    if is_index:
        url = f"{BASE}/api/option-chain-indices?symbol={sym}"
    else:
        url = f"{BASE}/api/option-chain-equities?symbol={sym}"
    js = _get_json(s, url, retries=CONFIG["data_sources"]["nse"]["retries"])
    return js or {}
