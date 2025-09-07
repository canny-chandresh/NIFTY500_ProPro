# src/_engine_utils.py
from __future__ import annotations
import numpy as np
import pandas as pd

NON_FEAT = {"Date","symbol","freq","asof_ts","regime_flag","y_1d",
            "live_source_equity","live_source_options","is_synth_options","data_age_min"}

def feature_cols(df: pd.DataFrame):
    return [c for c in df.columns if c not in NON_FEAT and not c.endswith("_is_missing")]

def last_per_symbol(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    return (df.sort_values(["symbol","Date"])
              .groupby("symbol", as_index=False)
              .tail(1))

def safe_winprob(x: float) -> float:
    return float(max(0.05, min(0.95, x)))
