# -*- coding: utf-8 -*-
"""
data_ingest.py
One-stop ingestion for daily + intraday (5m) using yfinance, with robust logging.
Writes:
  - datalake/per_symbol/<SYMBOL>.csv      (rolling OHLCV daily)
  - datalake/daily_hot.parquet            (joined latest daily rows)
  - datalake/intraday/5m/<SYMBOL>.csv     (today's 5m bars, if market day)
"""

from __future__ import annotations
import os, time, math, traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Dict, Any

import pandas as pd
import numpy as np

try:
    import yfinance as yf
except Exception:
    yf = None

from config import CONFIG

DL = Path(CONFIG["paths"]["datalake"])
PER = DL / "per_symbol"
INTRA5 = DL / "intraday" / "5m"
MACRO = DL / "macro"
LOGDIR = Path(CONFIG["paths"]["reports"]) / "debug"
for p in [DL, PER, INTRA5, MACRO, LOGDIR]:
    p.mkdir(parents=True, exist_ok=True)

def _utc_now():
    return datetime.now(timezone.utc).isoformat()

def _log(msg: str):
    print(f"[INGEST] {msg}")

def _save_debug(name: str, obj: Any):
    try:
        (LOGDIR / f"{name}.txt").write_text(str(obj))
    except Exception:
        pass

def _yf_download(symbol: str, period="2y", interval="1d"):
    if yf is None:
        raise RuntimeError("yfinance not installed")
    return yf.download(symbol, period=period, interval=interval, progress=False)

def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.reset_index().copy()
    if "Date" in df.columns:
        df.rename(columns={"Date": "date"}, inplace=True)
    if "Datetime" in df.columns:
        df.rename(columns={"Datetime": "datetime"}, inplace=True)
    # Standard columns
    for a, b in {
        "Open": "open", "High": "high", "Low": "low", "Close": "close",
        "Adj Close": "adj_close", "Volume": "volume"
    }.items():
        if a in df.columns:
            df.rename(columns={a: b}, inplace=True)
    return df

def backfill_daily(universe: List[str], period: str = "2y") -> Dict[str, Any]:
    """Daily OHLCV per symbol -> per_symbol/*.csv and daily_hot.parquet (joined latest)"""
    out = {"ok": True, "written": 0, "universe": len(universe)}
    latest_rows = []
    for sym in universe:
        try:
            df = _yf_download(sym, period=period, interval="1d")
            df = _normalize_df(df)
            if df.empty:
                _log(f"{sym}: no daily data")
                continue
            df["symbol"] = sym
            # persist per-symbol rolling file
            fp = PER / f"{sym}.csv"
            if fp.exists():
                try:
                    old = pd.read_csv(fp)
                    df = pd.concat([old, df]).drop_duplicates(subset=["date"], keep="last")
                except Exception:
                    pass
            df.to_csv(fp, index=False)
            out["written"] += 1
            latest_rows.append(df.sort_values("date").iloc[-1])
            time.sleep(float(CONFIG.get("ingest", {}).get("rate_limit_sec", 1.0)))
        except Exception as e:
            _log(f"{sym}: daily error: {e}")
            traceback.print_exc()

    if latest_rows:
        hot = pd.DataFrame(latest_rows)
        # ensure types
        hot["date"] = pd.to_datetime(hot["date"])
        hot["volume"] = pd.to_numeric(hot["volume"], errors="coerce").fillna(0).astype("int64")
        hot.to_parquet(DL / "daily_hot.parquet", index=False)
        _log(f"daily_hot.parquet rows={len(hot)}")
    else:
        _log("No latest rows -> daily_hot.parquet not updated")

    return out

def fetch_intraday_today(universe: List[str], interval="5m", max_symbols: int = 60) -> Dict[str, Any]:
    """Fetch *today's* intraday bars (5m) and store one CSV per symbol."""
    out = {"ok": True, "interval": interval, "written": 0}
    if interval != "5m":
        raise ValueError("Only 5m supported in this helper")

    # If weekend/holiday, yfinance may return empty sets — we still write an empty heartbeat file.
    count = 0
    for sym in universe[:max_symbols]:
        try:
            df = _yf_download(sym, period="5d", interval=interval)  # 5d window to catch today
            df = _normalize_df(df)
            if df.empty:
                (INTRA5 / f"{sym}.csv").write_text("") if not (INTRA5 / f"{sym}.csv").exists() else None
                continue
            df["datetime"] = pd.to_datetime(df["datetime"])
            today = df[df["datetime"].dt.date == df["datetime"].dt.date.max()].copy()
            if today.empty:
                (INTRA5 / f"{sym}.csv").write_text("") if not (INTRA5 / f"{sym}.csv").exists() else None
            else:
                today.to_csv(INTRA5 / f"{sym}.csv", index=False)
                out["written"] += 1
            count += 1
            time.sleep(float(CONFIG.get("ingest", {}).get("rate_limit_sec", 1.0)))
        except Exception as e:
            _log(f"{sym}: intraday error: {e}")
            traceback.print_exc()
    return out

def fetch_macro() -> Dict[str, Any]:
    """Fetch India VIX (proxy), DXY, USDINR; store to macro/macro.parquet."""
    out = {"ok": True, "rows": 0}
    series = {
        "india_vix": "^INDIAVIX",
        "dxy": "DX-Y.NYB",
        "usdinr": "USDINR=X"
    }
    rows = []
    for name, tkr in series.items():
        try:
            df = _yf_download(tkr, period="2y", interval="1d")
            df = _normalize_df(df)
            if df.empty: continue
            s = df[["date","close"]].copy()
            s["series"] = name
            rows.append(s)
            time.sleep(0.6)
        except Exception:
            traceback.print_exc()
    if rows:
        allm = pd.concat(rows, ignore_index=True)
        MACRO.mkdir(parents=True, exist_ok=True)
        allm.to_parquet(MACRO / "macro.parquet", index=False)
        out["rows"] = int(len(allm))
    return out

def run_all(universe: List[str]) -> Dict[str, Any]:
    """Convenience wrapper used by workflows."""
    if not universe:
        universe = CONFIG.get("universe", [])
    _log(f"run_all universe={len(universe)}")
    res_d = backfill_daily(universe, period=CONFIG.get("ingest",{}).get("daily_period","2y"))
    res_i = fetch_intraday_today(universe, interval="5m",
                                 max_symbols=int(CONFIG.get("ingest",{}).get("intraday",{}).get("max_symbols", 60)))
    res_m = fetch_macro()
    out = {
        "when": _utc_now(),
        "daily": res_d,
        "intraday": res_i,
        "macro": res_m,
        "paths": {
            "per_symbol": str(PER),
            "daily_hot": str(DL / "daily_hot.parquet"),
            "intraday_5m": str(INTRA5),
            "macro": str(MACRO / "macro.parquet"),
        }
    }
    _save_debug("ingest_summary", out)
    return out
