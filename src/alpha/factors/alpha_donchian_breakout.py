# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
import pandas as pd
from config import CONFIG

D = Path(CONFIG["paths"]["datalake"]) / "daily_hot.parquet"

def _donchian(sym: str, n=20) -> float:
    if not D.exists(): return 0.0
    try:
        df = pd.read_parquet(D)
        g = df[df["symbol"]==sym].sort_values("date").tail(n+1)
        if len(g) < n+1: return 0.0
        hi = g["high"].rolling(n).max().iloc[-1]
        lo = g["low"].rolling(n).min().iloc[-1]
        c = g["close"].iloc[-1]
        if hi==lo: return 0.0
        return float((c - lo)/(hi - lo) * 2 - 1)  # -1..+1 within channel
    except Exception:
        return 0.0

def compute(ff: pd.DataFrame, dlake) -> pd.DataFrame:
    import numpy as np
    out = pd.DataFrame(index=ff.index)
    out["alpha_donchian_breakout"] = [ _donchian(s) for s in ff["symbol"] ]
    return out
