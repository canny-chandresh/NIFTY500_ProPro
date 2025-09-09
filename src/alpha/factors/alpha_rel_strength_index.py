# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
import pandas as pd
import numpy as np
from config import CONFIG

D = Path(CONFIG["paths"]["datalake"]) / "daily_hot.parquet"

def _rs(sym: str, idx_sym: str = "^NSEI") -> float:
    if not D.exists(): return 0.0
    try:
        df = pd.read_parquet(D)
        s = df[df["symbol"]==sym].sort_values("date").tail(22)
        if s.empty: return 0.0
        # index proxy: use NIFTY50 from yfinance ticker stored under symbol "NIFTY50" if present
        idx = df[df["symbol"]==CONFIG.get("benchmarks",{}).get("nifty50_symbol","NIFTY50")].sort_values("date").tail(22)
        if idx.empty: return 0.0
        r_s = (s["close"].iloc[-1] - s["close"].iloc[0]) / (s["close"].iloc[0] or 1e-6)
        r_i = (idx["close"].iloc[-1] - idx["close"].iloc[0]) / (idx["close"].iloc[0] or 1e-6)
        return float(r_s - r_i)
    except Exception:
        return 0.0

def compute(ff: pd.DataFrame, dlake) -> pd.DataFrame:
    out = pd.DataFrame(index=ff.index)
    out["alpha_rel_strength_index"] = [ _rs(s) for s in ff["symbol"] ]
    return out
