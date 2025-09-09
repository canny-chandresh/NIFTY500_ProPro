# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
import pandas as pd
import numpy as np
from config import CONFIG

D = Path(CONFIG["paths"]["datalake"]) / "daily_hot.parquet"

def _entropy(sym: str, n=30) -> float:
    if not D.exists(): return 0.0
    df = pd.read_parquet(D)
    g = df[df["symbol"]==sym].sort_values("date").tail(n+1)
    if len(g) < n+1: return 0.0
    r = g["close"].pct_change().dropna()
    bins = pd.qcut(r, 10, labels=False, duplicates="drop")
    p = pd.Series(bins).value_counts(normalize=True)
    H = -(p * np.log(p + 1e-9)).sum()  # 0..~2.3
    # lower entropy -> more trend (positive alpha)
    return float((2.3 - H) / 2.3)

def compute(ff: pd.DataFrame, dlake) -> pd.DataFrame:
    out = pd.DataFrame(index=ff.index)
    out["alpha_entropy_vol"] = [ _entropy(s) for s in ff["symbol"] ]
    return out
