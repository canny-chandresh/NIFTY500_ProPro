# src/portfolio.py
from __future__ import annotations
import numpy as np
import pandas as pd

"""
Goal: convert ranked picks (with proba & risk stats) into position sizes.
Supports:
- equal: equal weight
- inv_vol: inverse volatility (uses rolling_vol or ATR if present)
- risk_parity: scale so each position contributes similar risk
- kelly: size by kelly fraction from (win_rate, payoff_ratio)
Inputs: picks DataFrame with at least:
  Symbol, proba, Entry, Target, SL
Optional columns:
  rolling_vol, atr, win_rate, payoff_ratio
Return: picks with 'size_pct' (sums to <= 1.0) and 'cap_reason' (if constrained)
"""

def _safe_series(x):
    return pd.to_numeric(x, errors="coerce").fillna(0.0)

def _kelly_f(win_rate: float, payoff: float) -> float:
    # Kelly f = p - (1-p)/b; clamp to [0,1]
    p = float(win_rate)
    b = max(1e-6, float(payoff))
    f = p - (1 - p)/b
    return float(np.clip(f, 0.0, 1.0))

def _risk_budget(weights, vols):
    # target equal risk contribution; Newton step (simple)
    w = np.array(weights, dtype=float)
    v = np.array(vols, dtype=float)
    v[v<=0] = np.nanmedian(v[v>0]) if np.any(v>0) else 1.0
    # normalize vols to avoid exploding
    v = v / np.nanmedian(v)
    # initialize inverse vol
    w = 1.0 / v
    w = w / w.sum()
    return w

def optimize_weights(
    picks: pd.DataFrame,
    method: str = "inv_vol",
    max_total_risk: float = 1.0,
    max_per_name: float = 0.25,
) -> pd.DataFrame:
    if picks is None or picks.empty:
        return picks
    d = picks.copy()
    n = len(d)

    # proxy risk
    vol = None
    if "rolling_vol" in d.columns:
        vol = _safe_series(d["rolling_vol"])
    elif "atr" in d.columns:
        vol = _safe_series(d["atr"] / d.get("Entry", 1.0))
    else:
        vol = pd.Series(1.0, index=d.index)

    if method == "equal":
        w = np.ones(n) / max(n,1)
    elif method == "inv_vol":
        v = vol.replace(0, np.nan)
        inv = 1.0 / v
        inv = inv.replace([np.inf, -np.inf], np.nan).fillna(inv[~inv.isna()].median() if (~inv.isna()).any() else 1.0)
        w = inv.values
        w = w / w.sum() if w.sum() > 0 else np.ones(n)/n
    elif method == "risk_parity":
        w = _risk_budget(np.ones(n)/n, vol.values)
    elif method == "kelly":
        wr = _safe_series(d.get("win_rate", d.get("proba", 0.55)))
        pr = _safe_series(d.get("payoff_ratio", (d.get("Target",1.0)-d.get("Entry",1.0)).abs() / (d.get("Entry",1.0)-d.get("SL",1e-6)).abs()))
        w = np.array([_kelly_f(wr.iloc[i], max(1e-6, pr.iloc[i])) for i in range(n)], dtype=float)
        # Normalize kelly to total risk budget
        if w.sum() > 0:
            w = w / w.sum()
        else:
            w = np.ones(n)/n
    else:
        # fallback
        w = np.ones(n)/max(n,1)

    # cap per name and total
    w = np.minimum(w, max_per_name)
    s = w.sum()
    if s > 0:
        w = (w / s) * min(1.0, max_total_risk)
    else:
        w = np.ones(n)/n

    d["size_pct"] = np.round(w, 4)
    d["cap_reason"] = np.where(d["size_pct"] >= max_per_name-1e-9, "per_name_cap", "")
    return d
