# -*- coding: utf-8 -*-
"""
feature_store.py
Builds a per-symbol feature frame from datalake inputs:
- daily_hot.parquet
- macro/macro.parquet
- (optional) intraday aggregates later (nightly)
Also computes ATR, EMAs, pivots, gap reasoning, and joins macro (VIX, etc.)
"""

from __future__ import annotations
from pathlib import Path
from typing import List, Dict, Any
import numpy as np
import pandas as pd

from config import CONFIG

DLAKE = Path(CONFIG["paths"]["datalake"])

def _ema(a: pd.Series, span: int) -> pd.Series:
    return a.ewm(span=span, adjust=False, min_periods=span).mean()

def _atr(df: pd.DataFrame, n: int = 14) -> pd.Series:
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)
    prev_close = close.shift(1)
    tr = pd.concat([
        (high - low).abs(),
        (high - prev_close).abs(),
        (low - prev_close).abs()
    ], axis=1).max(axis=1)
    return tr.rolling(n, min_periods=n).mean()

def _pivots(df: pd.DataFrame) -> pd.DataFrame:
    P = (df["high"] + df["low"] + df["close"]) / 3.0
    R1 = 2*P - df["low"]
    S1 = 2*P - df["high"]
    return pd.DataFrame({"pivot": P, "r1": R1, "s1": S1})

def _gap_reasoning(df: pd.DataFrame) -> pd.Series:
    prev_close = df["close"].shift(1)
    gp = (df["open"] - prev_close) / prev_close.replace(0, np.nan)
    # did we close into the gap (towards previous close)?
    close_in_gap = ((df["close"] - df["open"]) * (-np.sign(gp))).clip(lower=0)
    # normalize
    return (close_in_gap / df["close"].replace(0,np.nan)).fillna(0.0)

def _join_macro(ff: pd.DataFrame) -> pd.DataFrame:
    mp = DLAKE/"macro"/"macro.parquet"
    if not mp.exists():
        ff["india_vix"] = 0.0
        return ff
    macro = pd.read_parquet(mp)
    macro["date"] = pd.to_datetime(macro["date"]).dt.tz_localize(None)
    ff["date"] = pd.to_datetime(ff["date"]).dt.tz_localize(None)
    vix = macro[macro["series"]=="india_vix"][["date","value"]].rename(columns={"value":"india_vix"})
    out = ff.merge(vix, on="date", how="left")
    out["india_vix"] = out["india_vix"].ffill().fillna(0.0)
    return out

def get_feature_frame(universe: List[str]) -> pd.DataFrame:
    p = DLAKE / "daily_hot.parquet"
    if not p.exists():
        return pd.DataFrame(columns=["symbol","date","open","high","low","close","volume",
                                     "ema20","ema50","atr","atr_pct","pivot","gap_pct","close_in_gap"])
    df = pd.read_parquet(p)
    df = df[df["symbol"].isin(universe)].copy()
    df = df.sort_values(["symbol","date"])

    features=[]
    for sym, g in df.groupby("symbol"):
        g = g.copy()
        g["ema20"] = _ema(g["close"], 20)
        g["ema50"] = _ema(g["close"], 50)
        g["atr"] = _atr(g, 14)
        g["atr_pct"] = g["atr"] / g["close"].replace(0,np.nan)
        piv = _pivots(g)
        g = pd.concat([g.reset_index(drop=True), piv.reset_index(drop=True)], axis=1)
        prev_close = g["close"].shift(1)
        g["gap_pct"] = (g["open"] - prev_close) / prev_close.replace(0, np.nan)
        g["close_in_gap"] = _gap_reasoning(g)
        features.append(g)
    ff = pd.concat(features, ignore_index=True) if features else df

    ff = _join_macro(ff)
    # keep last available date per symbol
    last_idx = ff.groupby("symbol")["date"].idxmax()
    ff = ff.loc[last_idx].reset_index(drop=True)
    return ff
