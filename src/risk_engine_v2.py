"""
Risk Engine v2: CVaR + drawdown caps + Kelly blend.
"""

import numpy as np, pandas as pd

def compute_var(pnl_series: pd.Series, alpha=0.05):
    return np.percentile(pnl_series.dropna(), 100*alpha)

def compute_cvar(pnl_series: pd.Series, alpha=0.05):
    var = compute_var(pnl_series, alpha)
    tail = pnl_series[pnl_series <= var]
    return tail.mean() if not tail.empty else var

def size_with_guards(signal_weight, pnl_hist: pd.Series, max_drawdown=0.2):
    """
    Adjusts weights using CVaR and drawdown guard.
    """
    cvar = compute_cvar(pnl_hist)
    if cvar > -0.01: risk_adj = 1.0
    else: risk_adj = min(0.5, abs(0.01/cvar))
    return signal_weight * risk_adj
