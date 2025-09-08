# -*- coding: utf-8 -*-
"""
Alternative macro extractor: Gift Nifty, DXY, USDINR, etc. via yfinance baseline.
Writes datalake/discovery/derived/altmacro.parquet
"""

from __future__ import annotations
from pathlib import Path
import pandas as pd
from typing import Dict
from config import CONFIG

try:
    import yfinance as yf
except Exception:
    yf = None

DER = Path(CONFIG["paths"]["datalake"]) / "discovery" / "derived"
DER.mkdir(parents=True, exist_ok=True)

def fetch_altmacro() -> str:
    if yf is None: 
        return str(DER/"altmacro.parquet")
    tickers: Dict[str,str] = {
        "gift_nifty": "^GIFNIFTY",
        "dxy": "DX-Y.NYB",
        "usdinr": "USDINR=X"
    }
    rows=[]
    for name,tkr in tickers.items():
        try:
            df = yf.download(tkr, period="1y", interval="1d", progress=False)
            if df is None or df.empty: continue
            s = df["Close"].reset_index()
            s.columns = ["date","value"]
            s["series"] = name
            rows.append(s)
        except Exception:
            continue
    if rows:
        out = pd.concat(rows, ignore_index=True)
        p = DER/"altmacro.parquet"
        out.to_parquet(p, index=False)
        return str(p)
    return str(DER/"altmacro.parquet")
