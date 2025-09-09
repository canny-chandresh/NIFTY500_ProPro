# -*- coding: utf-8 -*-
"""
data_ingest.py — NSE-first ingestion with Yahoo fallback.
Writes:
  - datalake/per_symbol/<SYMBOL>.csv      (rolling daily)
  - datalake/daily_hot.parquet            (joined latest rows)
  - datalake/intraday/5m/<SYMBOL>.csv     (today 5m)
  - datalake/macro/macro.parquet          (INDIAVIX/DXY/INR via Yahoo)
"""
from __future__ import annotations
import os, time, traceback
from pathlib import Path
from typing import List, Dict, Any

import pandas as pd
import numpy as np

from config import CONFIG

# optional imports
try:
    import yfinance as yf
except Exception:
    yf = None

# NSE helper
try:
    from data_sources.nse_client import daily_equity as nse_daily, intraday_5m_today as nse_5m
except Exception:
    nse_daily = nse_5m = None

DL = Path(CONFIG["paths"]["datalake"])
PER = DL / "per_symbol"
INTRA5 = DL / "intraday" / "5m"
MACRO = DL / "macro"
RPTDBG = Path(CONFIG["paths"]["reports"]) / "debug"
for p in [DL, PER, INTRA5, MACRO, RPTDBG]:
    p.mkdir(parents=True, exist_ok=True)

def _log(msg: str):
    print(f"[INGEST] {msg}")

def _yahoo_daily(sym: str, period: str) -> pd.DataFrame:
    if yf is None: return pd.DataFrame()
    t = f"{sym}.NS" if not sym.endswith(".NS") else sym
    df = yf.download(t, period=period, interval="1d", progress=False)
    if df is None or df.empty: return pd.DataFrame()
    df = df.reset_index()
    if "Date" in df.columns: df.rename(columns={"Date":"date"}, inplace=True)
    df.rename(columns={"Open":"open","High":"high","Low":"low","Close":"close","Adj Close":"adj_close","Volume":"volume"}, inplace=True)
    df["symbol"] = sym.replace(".NS","")
    return df[["date","open","high","low","close","adj_close","volume","symbol"]]

def _yahoo_5m_today(sym: str, interval: str) -> pd.DataFrame:
    if yf is None: return pd.DataFrame()
    t = f"{sym}.NS" if not sym.endswith(".NS") else sym
    df = yf.download(t, period="5d", interval=interval, progress=False)
    if df is None or df.empty: return pd.DataFrame()
    df = df.reset_index().rename(columns={"Datetime":"datetime","Open":"open","High":"high","Low":"low","Close":"close","Adj Close":"adj_close","Volume":"volume"})
    if "datetime" not in df.columns: return pd.DataFrame()
    df["date"] = pd.to_datetime(df["datetime"]).dt.date
    today = df[df["date"] == df["date"].max()].copy()
    return today[["datetime","open","high","low","close","volume"]]

def backfill_daily(universe: List[str], period: str) -> Dict[str, Any]:
    src = CONFIG["data_sources"]["primary"]
    fb  = CONFIG["data_sources"]["fallback"]
    out = {"ok": True, "written": 0, "source_counts": {"nse":0,"yahoo":0}}
    latest = []
    for raw in universe:
        sym = raw.replace(".NS","")
        df = pd.DataFrame()
        # NSE first
        if src == "nse" and nse_daily is not None:
            try:
                df = nse_daily(sym)
                if not df.empty:
                    out["source_counts"]["nse"] += 1
                    # backfill via yahoo to add OHLC if available
                    ydf = _yahoo_daily(sym, period) if yf else pd.DataFrame()
                    if not ydf.empty:
                        # merge close from NSE with OHLC from Yahoo by date
                        df = pd.merge(ydf, df[["date","close"]], on="date", how="left", suffixes=("","_nse"))
                        df["close"] = df["close_nse"].fillna(df["close"])
                        df.drop(columns=[c for c in df.columns if c.endswith("_nse")], inplace=True)
            except Exception as e:
                _log(f"NSE daily fail {sym}: {e}")
        # Fallback
        if df.empty and fb == "yahoo":
            df = _yahoo_daily(sym, period)

        if df.empty:
            _log(f"{sym}: no daily data from any source")
            continue

        # persist per_symbol
        fp = PER / f"{sym}.csv"
        if fp.exists():
            try:
                old = pd.read_csv(fp)
                df = pd.concat([old, df]).drop_duplicates(subset=["date"], keep="last")
            except Exception:
                pass
        df.to_csv(fp, index=False)
        out["written"] += 1
        latest.append(df.sort_values("date").iloc[-1])
        time.sleep(float(CONFIG["data_sources"]["yahoo"]["rate_limit_sec"]))
    if latest:
        hot = pd.DataFrame(latest)
        hot["date"] = pd.to_datetime(hot["date"])
        # guard columns
        for c in ["open","high","low","close","adj_close","volume"]:
            if c not in hot.columns: hot[c] = np.nan if c!="volume" else 0
        hot.to_parquet(DL / "daily_hot.parquet", index=False)
        _log(f"daily_hot.parquet rows={len(hot)}")
    return out

def fetch_intraday_today(universe: List[str], interval="5m", max_symbols: int = 60) -> Dict[str, Any]:
    src = CONFIG["data_sources"]["primary"]
    fb  = CONFIG["data_sources"]["fallback"]
    out = {"ok": True, "written": 0, "source_counts": {"nse":0,"yahoo":0}}
    count = 0
    for raw in universe[:max_symbols]:
        sym = raw.replace(".NS","")
        df = pd.DataFrame()
        # NSE first
        if src == "nse" and nse_5m is not None:
            try:
                df = nse_5m(sym)
                if not df.empty:
                    out["source_counts"]["nse"] += 1
            except Exception as e:
                _log(f"NSE 5m fail {sym}: {e}")
        # Fallback
        if df.empty and fb == "yahoo":
            df = _yahoo_5m_today(sym, interval)
            if not df.empty: out["source_counts"]["yahoo"] += 1

        # write (even empty file once for heartbeat)
        fp = INTRA5 / f"{sym}.csv"
        if df.empty and not fp.exists():
            fp.write_text("")
        elif not df.empty:
            df.to_csv(fp, index=False)
            out["written"] += 1

        count += 1
        time.sleep(float(CONFIG["data_sources"]["yahoo"]["rate_limit_sec"]))
    return out

def fetch_macro() -> Dict[str, Any]:
    """Macro (INDIAVIX/DXY/INR) via Yahoo"""
    out = {"ok": True, "rows": 0}
    if yf is None: return out
    series = {"india_vix": "^INDIAVIX", "dxy": "DX-Y.NYB", "usdinr": "USDINR=X"}
    rows = []
    for name, tkr in series.items():
        try:
            df = yf.download(tkr, period=CONFIG["data_sources"]["yahoo"]["daily_period"], interval="1d", progress=False)
            if df is None or df.empty: continue
            s = df.reset_index()[["Date","Close"]].rename(columns={"Date":"date","Close":"close"})
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
    if not universe: universe = CONFIG.get("universe", [])
    _log(f"run_all NSE-first universe={len(universe)}")
    res_d = backfill_daily(universe, period=CONFIG["data_sources"]["yahoo"]["daily_period"])
    res_i = fetch_intraday_today(universe,
                                 interval=CONFIG["data_sources"]["nse"]["intraday_interval"],
                                 max_symbols=int(CONFIG["data_sources"]["nse"]["max_intraday_symbols_per_run"]))
    res_m = fetch_macro()
    out = {"daily": res_d, "intraday": res_i, "macro": res_m}
    (RPTDBG / "ingest_summary.txt").write_text(str(out))
    print(out)
    return out
