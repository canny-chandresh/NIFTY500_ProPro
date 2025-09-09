# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
import pandas as pd
import numpy as np
from config import CONFIG

D = Path(CONFIG["paths"]["datalake"]) / "daily_hot.parquet"

def _rsi(series: pd.Series, n=14) -> pd.Series:
    d = series.diff()
    up = d.clip(lower=0).rolling(n).mean()
    dn = (-d.clip(upper=0)).rolling(n).mean()
    rs = up / (dn.replace(0, 1e-9))
    return 100 - 100/(1+rs)

def _divergence(sym: str) -> float:
    if not D.exists(): return 0.0
    df = pd.read_parquet(D)
    g = df[df["symbol"]==sym].sort_values("date").tail(60)
    if len(g) < 30: return 0.0
    c = g["close"]
    r = _rsi(c)
    # higher high in price but lower high in RSI -> bearish (-)
    # lower low in price but higher low in RSI -> bullish (+)
    p_hh = c.iloc[-1] > c.rolling(20).max().iloc[-2]
    r_lh = r.iloc[-1] < r.rolling(20).max().iloc[-2]
    p_ll = c.iloc[-1] < c.rolling(20).min().iloc[-2]
    r_hl = r.iloc[-1] > r.rolling(20).min().iloc[-2]
    if p_hh and r_lh: return -0.7
    if p_ll and r_hl: return +0.7
    return 0.0

def compute(ff: pd.DataFrame, dlake) -> pd.DataFrame:
    out = pd.DataFrame(index=ff.index)
    out["alpha_rsi_divergence"] = [ _divergence(s) for s in ff["symbol"] ]
    return out
