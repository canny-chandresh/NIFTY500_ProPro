# src/microstructure.py
from __future__ import annotations
import pandas as pd
import numpy as np

"""
Lightweight intraday microstructure features from OHLCV (no paid feeds):
- vwap
- range_ratio (HL range vs close)
- choppiness proxy (rolling ATR / range)
- imbalance proxy (close->close return vs volume)
"""

def vwap(df: pd.DataFrame) -> pd.Series:
    price = (df["High"] + df["Low"] + df["Close"]) / 3.0
    vol = df["Volume"].replace(0, np.nan)
    return (price * vol).cumsum() / vol.cumsum()

def add_micro_features(df: pd.DataFrame, span: int = 20) -> pd.DataFrame:
    d = df.copy()
    d["vwap"] = vwap(d)
    d["range"] = (d["High"] - d["Low"]).abs()
    d["range_ratio"] = (d["range"] / d["Close"]).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    d["ret_cc"] = d["Close"].pct_change().fillna(0.0)
    d["imbalance"] = (d["ret_cc"].rolling(span).sum() * (d["Volume"].rolling(span).mean())).fillna(0.0)
    # choppiness proxy: ATR-like vs rolling range
    d["roll_range"] = d["range"].rolling(span).mean()
    d["roll_close"] = d["Close"].rolling(span).mean()
    d["choppy"] = (d["roll_range"] / d["roll_close"]).replace([np.inf,-np.inf], np.nan).fillna(0.0)
    return d
