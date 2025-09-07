# src/risk_engine.py
from __future__ import annotations
import numpy as np, pandas as pd

def rolling_var(returns: pd.Series, window=60, q=0.95) -> float:
    r = returns.dropna().tail(window)
    if len(r) < max(20, window//2): return 0.0
    return float(np.quantile(r, 1-q))

def kelly_fraction(winrate: float, rr: float) -> float:
    # rr = average_win / average_loss (abs)
    if rr <= 0: return 0.0
    p = winrate/100.0; b = rr
    f = (p*(1+b)-1)/b
    return max(0.0, min(1.0, f))

def size_with_guards(base_weight: float, ret_series: pd.Series,
                     winrate: float, rr: float, kelly_cap=0.33,
                     var_window=60, var_q=0.95) -> float:
    var = rolling_var(ret_series, window=var_window, q=var_q)  # per-trade R quantile
    k = min(kelly_cap, kelly_fraction(winrate, rr))
    safe = base_weight * (1 - min(0.5, var)) * (0.5 + 0.5*k)
    return float(max(0.0, safe))
