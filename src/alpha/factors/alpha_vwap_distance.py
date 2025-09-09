# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
import pandas as pd
import numpy as np
from config import CONFIG

D = Path(CONFIG["paths"]["datalake"]) / "daily_hot.parquet"

def _vwap_distance(sym: str) -> float:
    if not D.exists(): return 0.0
    try:
        df = pd.read_parquet(D)
        g = df[df["symbol"]==sym].sort_values("date").tail(60)
        if g.empty: return 0.0
        typ = (g["high"]+g["low"]+g["close"])/3.0
        v = g["volume"].replace(0,1.0)
        vwap = (typ*v).cumsum() / v.cumsum()
        last = g["close"].iloc[-1]
        return float((last - vwap.iloc[-1]) / (vwap.iloc[-1] or 1e-6))
    except Exception:
        return 0.0

def compute(ff: pd.DataFrame, dlake) -> pd.DataFrame:
    out = pd.DataFrame(index=ff.index)
    out["alpha_vwap_distance"] = [ _vwap_distance(s) for s in ff["symbol"] ]
    return out
