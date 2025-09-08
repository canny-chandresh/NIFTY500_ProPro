# src/risk_suite_v25.py
from __future__ import annotations
import json
from pathlib import Path
from typing import Dict

import numpy as np
import pandas as pd

def _paths(cfg: Dict):
    rep = Path(cfg.get("paths", {}).get("reports","reports")) / "risk_v25"
    rep.mkdir(parents=True, exist_ok=True)
    return rep

def _series_daily_pnl(dl: str) -> pd.Series:
    p = Path(dl) / "paper_trades.csv"
    if not p.exists(): return pd.Series(dtype=float)
    df = pd.read_csv(p, parse_dates=["timestamp"])
    if df.empty: return pd.Series(dtype=float)
    return df.groupby(df["timestamp"].dt.date)["pnl"].sum()

def _VaR(ser: pd.Series, alpha: float=0.05) -> float:
    ser = ser.dropna()
    if ser.empty: return 0.0
    return float(np.quantile(ser, alpha))

def _CVaR(ser: pd.Series, alpha: float=0.05) -> float:
    ser = ser.dropna()
    if ser.empty: return 0.0
    cutoff = np.quantile(ser, alpha)
    tail = ser[ser <= cutoff]
    return float(tail.mean()) if len(tail) else 0.0

def portfolio_risk_report(cfg: Dict) -> Dict:
    rep = _paths(cfg)
    dl = cfg.get("paths", {}).get("datalake","datalake")
    s = _series_daily_pnl(dl)
    var95  = _VaR(s, 0.05)
    cvar95 = _CVaR(s, 0.05)
    out = {"ok": True, "days": int(len(s)), "VaR_95": var95, "CVaR_95": cvar95,
           "pnl_total": float(s.sum()), "pnl_mean": float(s.mean() if len(s) else 0.0)}
    (rep / "portfolio.json").write_text(json.dumps(out, indent=2), encoding="utf-8")
    return out

def tighten_positions(cfg: Dict, picks: pd.DataFrame, hit_rate_pct: float) -> pd.DataFrame:
    # laddered: <25% => keep top 50%; <30% => keep top 70%; else keep all
    if picks is None or picks.empty: return picks
    if hit_rate_pct < 25.0:
        return picks.sort_values("Confidence", ascending=False).head(max(1, int(len(picks)*0.5)))
    if hit_rate_pct < 30.0:
        return picks.sort_values("Confidence", ascending=False).head(max(1, int(len(picks)*0.7)))
    return picks
