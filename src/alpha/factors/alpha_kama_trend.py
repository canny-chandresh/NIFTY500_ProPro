# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
import pandas as pd
import numpy as np
from config import CONFIG

D = Path(CONFIG["paths"]["datalake"]) / "daily_hot.parquet"

def _kama(x: pd.Series, n=10, fast=2, slow=30) -> pd.Series:
    # Simplified KAMA
    change = x.diff(n).abs()
    volatility = x.diff().abs().rolling(n).sum()
    er = (change / (volatility.replace(0, 1e-9))).fillna(0)
    sc = (er*(2/(fast+1)) + (1-er)*(2/(slow+1)))**2
    kama = [x.iloc[0]]
    for i in range(1, len(x)):
        kama.append(kama[-1] + sc.iloc[i] * (x.iloc[i]-kama[-1]))
    return pd.Series(kama, index=x.index)

def _slope(sym: str) -> float:
    if not D.exists(): return 0.0
    try:
        df = pd.read_parquet(D)
        g = df[df["symbol"]==sym].sort_values("date").tail(40)
        if len(g) < 20: return 0.0
        k = _kama(g["close"])
        return float((k.iloc[-1] - k.iloc[-10]) / (abs(k.iloc[-10]) + 1e-6))
    except Exception:
        return 0.0

def compute(ff: pd.DataFrame, dlake) -> pd.DataFrame:
    out = pd.DataFrame(index=ff.index)
    out["alpha_kama_trend"] = [ _slope(s) for s in ff["symbol"] ]
    return out
