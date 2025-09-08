# -*- coding: utf-8 -*-
"""
Generic market extractor (yfinance baseline). Writes under datalake/discovery/raw/market/.
"""

from __future__ import annotations
from pathlib import Path
import pandas as pd
from typing import List
from config import CONFIG

try:
    import yfinance as yf
except Exception:
    yf = None

D = Path(CONFIG["paths"]["datalake"]) / "discovery" / "raw" / "market"
D.mkdir(parents=True, exist_ok=True)

def fetch_symbols(symbols: List[str], period="6mo", interval="1d") -> str:
    if yf is None: return ""
    for s in symbols:
        try:
            df = yf.download(s, period=period, interval=interval, progress=False)
            if df is None or df.empty: continue
            df.reset_index().to_parquet(D/f"{s.replace(':','_')}_{interval}.parquet", index=False)
        except Exception:
            continue
    return str(D)
