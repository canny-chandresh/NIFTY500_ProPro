# src/model_selector.py
"""
Lightweight model selector used by pipeline.run_paper_session().
- Reads daily prices from the datalake (parquet or CSV).
- Builds a simple momentum score as a stand-in for the full ML.
- Returns a DataFrame with columns required by the pipeline:
  Symbol, Entry, SL, Target, proba, Reason
"""

from __future__ import annotations
import os
import pandas as pd
import numpy as np
from typing import Tuple
from config import DL, CONFIG


def _load_daily_prices() -> pd.DataFrame:
    """Load daily OHLCV from datalake. Supports parquet or CSV.
       Expected columns: Date, Symbol, Open, High, Low, Close, (optional Volume)
    """
    fp = DL("daily_equity")
    if os.path.exists(fp):
        try:
            df = pd.read_parquet(fp)
            # ensure Date typed
            if "Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Date"])
            return df
        except Exception:
            pass

    fp_csv = DL("daily_equity_csv")
    if os.path.exists(fp_csv):
        try:
            df = pd.read_csv(fp_csv, parse_dates=["Date"])
            return df
        except Exception:
            pass

    return pd.DataFrame()


def _basic_momentum_scores(df: pd.DataFrame, lookback: int = 5) -> pd.DataFrame:
    """Create a simple momentum score per symbol using recent % change."""
    if df.empty:
        return pd.DataFrame(columns=["Symbol", "score", "Entry", "SL", "Target", "Reason", "proba"])

    # Keep last N days per symbol
    df = df.sort_values(["Symbol", "Date"]).copy()
    df["ret1"] = df.groupby("Symbol")["Close"].pct_change()
    # rolling mean of daily returns as momentum
    df["mom"] = df.groupby("Symbol")["ret1"].rolling(lookback, min_periods=max(2, lookback // 2)).mean().reset_index(level=0, drop=True)

    # last row per symbol
    last = df.groupby("Symbol").tail(1).reset_index(drop=True)
    last = last[["Symbol", "Close", "High", "Low", "mom"]].rename(columns={"Close": "Entry"})
    last["score"] = last["mom"].fillna(0.0)

    # Simple SL/Target around Entry
    # (You can later swap these with ATR-based or ML-driven levels)
    last["SL"] = last["Entry"] * 0.98
    last["Target"] = last["Entry"] * 1.02

    # Convert score into a pseudo-probability in [0.4, 0.75]
    # to avoid extreme numbers when momentum is tiny.
    s = last["score"].fillna(0.0)
    s_norm = (s - s.quantile(0.1)) / max(1e-9, (s.quantile(0.9) - s.quantile(0.1)))
    last["proba"] = (0.575 + 0.175 * s_norm).clip(0.40, 0.75)

    # Reason text (the pipeline will later append S/R pivot rules string)
    last["Reason"] = np.where(last["score"] > 0, "Momentum up", "Momentum flat/down")

    # Sort best-first
    out = last.sort_values("proba", ascending=False)[
        ["Symbol", "Entry", "SL", "Target", "proba", "Reason"]
    ].reset_index(drop=True)
    return out


def choose_and_predict_full() -> Tuple[pd.DataFrame, str]:
    """
    Entry point called by pipeline.run_paper_session().
    Returns:
      - DataFrame with columns [Symbol, Entry, SL, Target, proba, Reason]
      - a string tag for which engine produced the recos (e.g., "light")
    """
    df = _load_daily_prices()
    if df.empty:
        # Nothing to score yet — return empty and let pipeline handle gracefully.
        return pd.DataFrame(columns=["Symbol", "Entry", "SL", "Target", "proba", "Reason"]), "light"

    # (Optional) restrict to your investible universe if present
    # e.g., drop index rows like NIFTY50 etc. Keep only equities
    # Detect index symbols heuristically:
    bad = {"NIFTY50", "^NSEI", "NIFTY.NS", "BANKNIFTY", "^NSEBANK", "NIFTYBANK.NS"}
    if "Symbol" in df.columns:
        df = df[~df["Symbol"].astype(str).str.upper().isin(bad)].copy()

    recos = _basic_momentum_scores(df, lookback=5)

    # If sector cap is enabled in CONFIG, the sector cap will be applied later
    # by pipeline.apply_sector_cap() so we just return the scored list.
    which = "light"  # tag used in Telegram text
    return recos, which
