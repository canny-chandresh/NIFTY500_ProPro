# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
import pandas as pd
import numpy as np
from config import CONFIG

D = Path(CONFIG["paths"]["datalake"]) / "daily_hot.parquet"

def _bb_pos(sym: str, n=20, k=2.0) -> float:
    if not D.exists(): return 0.0
    df = pd.read_parquet(D)
    g = df[df["symbol"]==sym].sort_values("date").tail(n+1)
    if len(g) < n+1: return 0.0
    c = g["close"]
    m = c.rolling(n).mean().iloc[-1]
    s = c.rolling(n).std().iloc[-1]
    up, dn = m + k*s, m - k*s
    if up==dn: return 0.0
    return float((c.iloc[-1] - dn) / (up - dn) * 2 - 1)  # -1..+1

def compute(ff: pd.DataFrame, dlake) -> pd.DataFrame:
    out = pd.DataFrame(index=ff.index)
    out["alpha_bb_position"] = [ _bb_pos(s) for s in ff["symbol"] ]
    return out
