# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
import pandas as pd
import numpy as np
from config import CONFIG

D = Path(CONFIG["paths"]["datalake"]) / "daily_hot.parquet"

def _avwap_ytd(sym: str) -> float:
    if not D.exists(): return 0.0
    df = pd.read_parquet(D)
    g = df[df["symbol"]==sym].sort_values("date")
    if g.empty: return 0.0
    y = g["date"].max().year
    ytd = g[g["date"].dt.year == y]
    if ytd.empty: return 0.0
    typ = (ytd["high"]+ytd["low"]+ytd["close"])/3.0
    v = ytd["volume"].replace(0,1.0)
    avwap = (typ*v).cumsum()/v.cumsum()
    last = ytd["close"].iloc[-1]
    return float((last - avwap.iloc[-1]) / (avwap.iloc[-1] or 1e-6))

def compute(ff: pd.DataFrame, dlake) -> pd.DataFrame:
    out = pd.DataFrame(index=ff.index)
    out["alpha_avwap_ytd"] = [ _avwap_ytd(s) for s in ff["symbol"] ]
    return out
