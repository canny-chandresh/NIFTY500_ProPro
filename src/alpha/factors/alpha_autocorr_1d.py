# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
import pandas as pd
import numpy as np
from config import CONFIG

D = Path(CONFIG["paths"]["datalake"]) / "daily_hot.parquet"

def _ac1(sym: str, n=20) -> float:
    if not D.exists(): return 0.0
    df = pd.read_parquet(D)
    g = df[df["symbol"]==sym].sort_values("date").tail(n+1)
    if len(g) < n+1: return 0.0
    r = g["close"].pct_change().dropna()
    if len(r) < 5: return 0.0
    return float(r.autocorr(lag=1) or 0.0)

def compute(ff: pd.DataFrame, dlake) -> pd.DataFrame:
    out = pd.DataFrame(index=ff.index)
    out["alpha_autocorr_1d"] = [ _ac1(s) for s in ff["symbol"] ]
    return out
