# -*- coding: utf-8 -*-
"""
data_ingest.py — NSE-first ingestion with Yahoo fallback.
Writes:
  - datalake/per_symbol/<SYMBOL>.csv      (rolling daily)
  - datalake/daily_hot.parquet            (joined latest rows across universe)
  - datalake/intraday/5m/<SYMBOL>.csv     (today 5m)
  - datalake/macro/macro.parquet          (INDIAVIX/DXY/INR via Yahoo)
Robust to missing columns (fills defaults) and won’t crash the workflow.
"""

from __future__ import annotations
import os, time, traceback
from pathlib import Path
from typing import List, Dict, Any

import pandas as pd
import numpy as np

from config import CONFIG

# Optional imports
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

def _safe_to_numeric(x, default=np.nan):
    try:
        return pd.to_numeric(x, errors="coerce")
    except Exception:
        # if x is missing or scalar, return default
        return pd.Series([default])

def _ensure_daily_cols(df: pd.DataFrame, sym: str) -> pd.DataFrame:
    """
    Ensure daily DF has date, open, high, low, close, adj_close, volume, symbol.
    Fill sensible defaults when absent.
    """
    if df is None or df.empty:
        return pd.DataFrame()

    # normalize date column
    if "date" not in df.columns:
        if "Date" in df.columns:
            df = df.rename(columns={"Date": "date"})
        elif "datetime" in df.columns:
            df["date"] = pd.to_datetime(df["datetime"]).dt.date
        else:
            # cannot recover date; bail
            return pd.DataFrame()

    # create missing numeric columns
    for c in ["open", "high", "low", "close", "adj_close", "volume"]:
        if c not in df.columns:
            df[c] = np.nan if c != "volume" else 0

    # coerce numerics
    for c in ["open", "high", "low", "close", "adj_close", "volume"]:
        try:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        except Exception:
            pass

    # symbol
    df["symbol"] = sym.replace(".NS", "")

    # keep only expected cols
    keep = ["date", "open", "high", "low", "close", "adj_close", "volume", "symbol"]
    df = df[[c for c in keep if c in df.columns]].copy()
    # ensure date is datetime (not object)
    df["date"] = pd.to_datetime(df["date"])
    return df

def _yahoo_daily(sym: str, period: str) -> pd.DataFrame:
    if yf is None:
        return pd.DataFrame()
    t = f"{sym}.NS" if not sym.endswith(".NS") else sym
    try:
        # keep raw OHLC/Volume (no auto adjust)
        df = yf.download(t, period=period, interval="1d", progress=False, auto_adjust=False)
        if df is None or df.empty:
            return pd.DataFrame()
        df = df.reset_index()
        df.rename(columns={
            "Date": "date", "Open": "open", "High": "high", "Low": "low",
            "Close": "close", "Adj Close": "adj_close", "Volume": "volume"
        }, inplace=True)
        return _ensure_daily_cols(df, sym)
    except Exception:
        traceback.print_exc()
        return pd.DataFrame()

def _yahoo_5m_today(sym: str, interval: str) -> pd.DataFrame:
    if yf is None:
        return pd.DataFrame()
    t = f"{sym}.NS" if not sym.endswith(".NS") else sym
    try:
        df = yf.download(t, period="5d", interval=interval, progress=False, auto_adjust=False)
        if df is None or df.empty:
            return pd.DataFrame()
        df = df.reset_index().rename(columns={
            "Datetime": "datetime", "Open": "open", "High": "high", "Low": "low",
            "Close": "close", "Adj Close": "adj_close", "Volume": "volume"
        })
        if "datetime" not in df.columns:
            return pd.DataFrame()
        df["date"] = pd.to_datetime(df["datetime"]).dt.date
        today = df[df["date"] == df["date"].max()].copy()
        return today[["datetime", "open", "high", "low", "close", "volume"]]
    except Exception:
        traceback.print_exc()
        return pd.DataFrame()

def backfill_daily(universe: List[str], period: str) -> Dict[str, Any]:
    src = CONFIG["data_sources"]["primary"]
    fb  = CONFIG["data_sources"]["fallback"]
    out = {"ok": True, "written": 0, "source_counts": {"nse": 0, "yahoo": 0}, "errors": []}
    latest_rows = []

    for raw in universe:
        sym = raw.replace(".NS", "")
        df_daily = pd.DataFrame()

        # 1) Try NSE daily (close), then enrich with Yahoo OHLC if available
        if src == "nse" and nse_daily is not None:
            try:
                ndf = nse_daily(sym)  # expect columns: date, close, symbol
                if ndf is not None and not ndf.empty:
                    out["source_counts"]["nse"] += 1
                    ydf = _yahoo_daily(sym, period) if yf else pd.DataFrame()
                    if ydf is not None and not ydf.empty:
                        # merge NSE close into Yahoo OHLC by date
                        merged = ydf.merge(
                            ndf[["date", "close"]].rename(columns={"close": "close_nse"}),
                            on="date", how="left"
                        )
                        merged["close"] = merged["close_nse"].fillna(merged["close"])
                        if "close_nse" in merged.columns:
                            merged.drop(columns=["close_nse"], inplace=True)
                        df_daily = merged
                    else:
                        # Only NSE close is available — synthesize minimal OHLC
                        df_daily = _ensure_daily_cols(ndf, sym)
                        for c in ["open", "high", "low"]:
                            df_daily[c] = df_daily["close"]
                        df_daily["adj_close"] = df_daily["close"]
                        df_daily["volume"] = df_daily.get("volume", 0).fillna(0)
            except Exception as e:
                out["errors"].append(f"NSE daily {sym}: {e}")
                traceback.print_exc()

        # 2) Fallback to Yahoo if still empty
        if (df_daily is None or df_daily.empty) and fb == "yahoo":
            df_daily = _yahoo_daily(sym, period)
            if df_daily is not None and not df_daily.empty:
                out["source_counts"]["yahoo"] += 1

        if df_daily is None or df_daily.empty:
            _log(f"{sym}: no daily data from any source")
            continue

        # Persist per_symbol CSV (dedupe by date)
        fp = PER / f"{sym}.csv"
        try:
            if fp.exists():
                old = pd.read_csv(fp, parse_dates=["date"])
                df_daily = pd.concat([old, df_daily], ignore_index=True)
                df_daily = df_daily.drop_duplicates(subset=["date"], keep="last")
        except Exception:
            traceback.print_exc()
        df_daily.sort_values("date", inplace=True)
        df_daily.to_csv(fp, index=False)
        out["written"] += 1

        # Collect latest row for daily_hot
        try:
            latest_rows.append(df_daily.iloc[-1])
        except Exception:
            pass

        # polite throttle to not hammer Yahoo if used
        time.sleep(float(CONFIG["data_sources"]["yahoo"]["rate_limit_sec"]))

    # Write daily_hot.parquet
    try:
        if latest_rows:
            hot = pd.DataFrame(latest_rows)
            # make sure numeric types are numeric
            for c in ["open", "high", "low", "close", "adj_close", "volume"]:
                if c not in hot.columns:
                    hot[c] = np.nan if c != "volume" else 0
                else:
                    hot[c] = pd.to_numeric(hot[c], errors="coerce")
            hot["volume"] = hot["volume"].fillna(0).astype("int64", errors="ignore")
            hot.to_parquet(DL / "daily_hot.parquet", index=False)
            _log(f"daily_hot.parquet rows={len(hot)}")
    except Exception as e:
        out["errors"].append(f"write daily_hot: {e}")
        traceback.print_exc()

    # Write a summary log
    (RPTDBG / "ingest_summary.txt").write_text(str(out))
    return out

def fetch_intraday_today(universe: List[str], interval="5m", max_symbols: int = 60) -> Dict[str, Any]:
    src = CONFIG["data_sources"]["primary"]
    fb  = CONFIG["data_sources"]["fallback"]
    out = {"ok": True, "written": 0, "source_counts": {"nse": 0, "yahoo": 0}, "errors": []}
    count = 0

    for raw in universe[:max_symbols]:
        sym = raw.replace(".NS", "")
        df = pd.DataFrame()

        # NSE 5m first
        if src == "nse" and nse_5m is not None:
            try:
                df = nse_5m(sym)
                if df is not None and not df.empty:
                    out["source_counts"]["nse"] += 1
            except Exception as e:
                out["errors"].append(f"NSE 5m {sym}: {e}")
                traceback.print_exc()

        # Fallback Yahoo 5m
        if (df is None or df.empty) and fb == "yahoo":
            df = _yahoo_5m_today(sym, interval)
            if df is not None and not df.empty:
                out["source_counts"]["yahoo"] += 1

        # Persist (even an empty file once, as heartbeat)
        fp = INTRA5 / f"{sym}.csv"
        try:
            if df is None or df.empty:
                if not fp.exists():
                    fp.write_text("")
            else:
                df.to_csv(fp, index=False)
                out["written"] += 1
        except Exception as e:
            out["errors"].append(f"write intra5 {sym}: {e}")
            traceback.print_exc()

        count += 1
        time.sleep(float(CONFIG["data_sources"]["yahoo"]["rate_limit_sec"]))

    (RPTDBG / "ingest_intraday_summary.txt").write_text(str(out))
    return out

def fetch_macro() -> Dict[str, Any]:
    """Macro (INDIAVIX/DXY/INR) via Yahoo."""
    out = {"ok": True, "rows": 0, "errors": []}
    if yf is None:
        return out
    series = {"india_vix": "^INDIAVIX", "dxy": "DX-Y.NYB", "usdinr": "USDINR=X"}
    rows = []
    for name, tkr in series.items():
        try:
            df = yf.download(tkr, period=CONFIG["data_sources"]["yahoo"]["daily_period"], interval="1d",
                             progress=False, auto_adjust=False)
            if df is None or df.empty:
                continue
            s = df.reset_index()[["Date", "Close"]].rename(columns={"Date": "date", "Close": "close"})
            s["series"] = name
            rows.append(s)
            time.sleep(0.6)
        except Exception as e:
            out["errors"].append(f"macro {name}: {e}")
            traceback.print_exc()
    if rows:
        allm = pd.concat(rows, ignore_index=True)
        MACRO.mkdir(parents=True, exist_ok=True)
        allm.to_parquet(MACRO / "macro.parquet", index=False)
        out["rows"] = int(len(allm))
    (RPTDBG / "ingest_macro_summary.txt").write_text(str(out))
    return out

def run_all(universe: List[str]) -> Dict[str, Any]:
    if not universe:
        universe = CONFIG.get("universe", [])
    _log(f"run_all universe={len(universe)}")

    # Use the longer Yahoo period for enrichment/backfill
    period = CONFIG["data_sources"]["yahoo"].get("daily_period", CONFIG.get("ingest", {}).get("daily_period", "2y"))

    res_d = backfill_daily(universe, period=period)
    res_i = fetch_intraday_today(
        universe,
        interval=CONFIG["data_sources"]["nse"].get("intraday_interval", "5m"),
        max_symbols=int(CONFIG["data_sources"]["nse"].get("max_intraday_symbols_per_run", 60)),
    )
    res_m = fetch_macro()
    out = {"daily": res_d, "intraday": res_i, "macro": res_m}
    (RPTDBG / "ingest_summary.txt").write_text(str(out))
    print(out)
    return out
