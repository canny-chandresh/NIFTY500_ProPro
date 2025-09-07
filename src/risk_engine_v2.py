# src/risk_engine_v2.py
from __future__ import annotations
import json
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd

def _paths(cfg: Dict):
    rep = Path(cfg.get("paths", {}).get("reports", "reports")) / "risk"
    rep.mkdir(parents=True, exist_ok=True)
    return rep

def _cfg(cfg: Dict):
    risk = cfg.get("risk", {})
    ks = cfg.get("kill_switch", {})
    return {
        "cvar_alpha": float(risk.get("cvar_alpha", 0.05)),
        "per_trade_risk_pct": float(risk.get("per_trade_risk_pct", 0.01)),
        "max_drawdown_portfolio": float(risk.get("max_drawdown_portfolio", 0.25)),
        "ladder": {
            # laddered thresholds: tighten when hit-rate deteriorates
            "tier1_floor": float(ks.get("min_hit_rate_pct", 30.0)),  # 30%
            "tier2_floor": float(ks.get("min_hit_rate_pct", 30.0)) - 5.0,  # 25%
        }
    }

def _portfolio_metrics(dl_path: str) -> Dict:
    p = Path(dl_path) / "paper_trades.csv"
    if not p.exists():
        return {"hit_rate": None, "pnl_sum": None, "days": 0}
    df = pd.read_csv(p, parse_dates=["timestamp"])
    if df.empty:
        return {"hit_rate": None, "pnl_sum": 0.0, "days": 0}
    wins = float((df.get("pnl", 0) > 0).mean())
    return {"hit_rate": wins * 100.0, "pnl_sum": float(df.get("pnl", 0).sum()), "days": int(df["timestamp"].dt.date.nunique())}

def historical_var(returns: pd.Series, alpha: float = 0.05) -> float:
    returns = pd.Series(returns).dropna()
    if returns.empty:
        return 0.0
    return float(np.quantile(returns, alpha))

def pretrade_filter(cfg: Dict, picks: pd.DataFrame, df_last: pd.DataFrame) -> pd.DataFrame:
    """
    Apply pre-trade risk guards:
    - exposure cap per sector (already handled elsewhere)
    - laddered kill switch if rolling hit-rate is poor: downsize or drop lowest-confidence picks
    """
    rep = _paths(cfg)
    r = _cfg(cfg)
    dl = cfg.get("paths", {}).get("datalake", "datalake")
    metrics = _portfolio_metrics(dl)

    hr = metrics.get("hit_rate") or 50.0
    tier1, tier2 = r["ladder"]["tier1_floor"], r["ladder"]["tier2_floor"]

    mode = "normal"
    drop_n = 0
    if hr < tier2:
        mode = "severe"
        drop_n = max(1, int(len(picks) * 0.4))  # drop bottom 40%
    elif hr < tier1:
        mode = "tight"
        drop_n = max(1, int(len(picks) * 0.2))  # drop bottom 20%

    picks2 = picks.copy()
    if drop_n > 0 and "Confidence" in picks2.columns:
        picks2 = picks2.sort_values("Confidence", ascending=False).head(len(picks2) - drop_n)

    report = {
        "pretrade_mode": mode,
        "hit_rate_pct": hr,
        "dropped": int(drop_n),
        "after_count": int(len(picks2))
    }
    (rep / "pretrade.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    return picks2

def posttrade_report(cfg: Dict) -> Dict:
    rep = _paths(cfg)
    dl = cfg.get("paths", {}).get("datalake", "datalake")
    p = Path(dl) / "paper_trades.csv"
    if not p.exists():
        out = {"ok": True, "note": "no_trades"}
        (rep / "posttrade.json").write_text(json.dumps(out, indent=2), encoding="utf-8")
        return out

    df = pd.read_csv(p, parse_dates=["timestamp"])
    daily = df.groupby(df["timestamp"].dt.date)["pnl"].sum()
    var_95 = historical_var(daily, 0.05)
    out = {
        "ok": True,
        "days": int(len(daily)),
        "pnl_total": float(daily.sum()),
        "pnl_avg_day": float(daily.mean() if len(daily) else 0.0),
        "VaR_95_daily": float(var_95),
    }
    (rep / "posttrade.json").write_text(json.dumps(out, indent=2), encoding="utf-8")
    return out
