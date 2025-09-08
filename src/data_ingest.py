# -*- coding: utf-8 -*-
"""
data_ingest.py
Unified fetchers for:
- daily OHLCV (yfinance)
- intraday 5m (yfinance best-effort)
- macro (INDIAVIX, DXY, USDINR, GIFT NIFTY, etc.)
- options chain snapshots (placeholder; safely skips when blocked)
All writes go under datalake/.
"""

from __future__ import annotations
import time, json
from pathlib import Path
from typing import List, Dict, Any
import pandas as pd

from config import CONFIG

try:
    import yfinance as yf
except Exception:
    yf = None

DLAKE = Path(CONFIG["paths"]["datalake"])
(DLAKE / "intraday" / "5m").mkdir(parents=True, exist_ok=True)
(DLAKE / "macro").mkdir(parents=True, exist_ok=True)
(DLAKE / "options_chain").mkdir(parents=True, exist_ok=True)

def _rl_sleep():
    time.sleep(float(CONFIG.get("ingest",{}).get("rate_limit_sec", 1.2)))

def fetch_daily(universe: List[str]) -> Path:
    if not CONFIG.get("ingest",{}).get("daily",{}).get("enabled", True):
        return DLAKE / "daily_hot.parquet"
    lb = int(CONFIG["ingest"]["daily"].get("lookback_days", 750))
    frames=[]
    for sym in universe:
        if yf is None: break
        try:
            _rl_sleep()
            df = yf.download(sym + ".NS", period=f"{lb}d", interval="1d", auto_adjust=False, progress=False)
            if df is None or df.empty: continue
            df = df.rename(columns=str.lower).reset_index().rename(columns={"index":"date"})
            df["symbol"] = sym
            frames.append(df)
        except Exception:
            continue
    if frames:
        out = pd.concat(frames, ignore_index=True)
        p = DLAKE / "daily_hot.parquet"
        out.to_parquet(p, index=False)
        return p
    return DLAKE / "daily_hot.parquet"

def fetch_intraday_5m(universe: List[str]) -> None:
    cfg = CONFIG.get("ingest",{}).get("intraday",{})
    if not cfg.get("enabled", True): return
    if yf is None: return
    period = cfg.get("period","5d")
    max_syms = int(cfg.get("max_symbols", 200))
    for sym in universe[:max_syms]:
        try:
            _rl_sleep()
            df = yf.download(sym + ".NS", period=period, interval="5m", auto_adjust=False, progress=False)
            if df is None or df.empty: continue
            df = df.rename(columns=str.lower).reset_index().rename(columns={"index":"datetime"})
            df["symbol"] = sym
            fp = DLAKE / "intraday" / "5m" / f"{sym}.csv"
            df.to_csv(fp, index=False)
        except Exception:
            continue

def fetch_macro() -> None:
    if not CONFIG.get("ingest",{}).get("macro",{}).get("enabled", True): return
    if yf is None: return
    tickers = CONFIG["ingest"]["macro"].get("tickers", {})
    rows=[]
    for name,tkr in tickers.items():
        try:
            _rl_sleep()
            df = yf.download(tkr, period="2y", interval="1d", auto_adjust=False, progress=False)
            if df is None or df.empty: continue
            s = df["Close"].reset_index().rename(columns={"Close":"value","index":"date"})
            s["series"] = name
            rows.append(s)
        except Exception:
            continue
    if rows:
        out = pd.concat(rows, ignore_index=True)
        out.to_parquet(DLAKE/"macro"/"macro.parquet", index=False)

def fetch_options_chain() -> None:
    """
    Placeholder: we keep this a no-op unless you wire a provider.
    We still create a tiny heartbeat file so downstream knows the step ran.
    """
    hb = {"ok": False, "why": "no provider", "ts": pd.Timestamp.utcnow().isoformat()}
    (DLAKE/"options_chain"/"heartbeat.json").write_text(json.dumps(hb, indent=2))

def run_all(universe: List[str]) -> Dict[str, Any]:
    p = fetch_daily(universe)
    fetch_intraday_5m(universe)
    fetch_macro()
    fetch_options_chain()
    return {"daily": str(p)}
