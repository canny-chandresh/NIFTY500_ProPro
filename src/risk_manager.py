# src/risk_manager.py
from __future__ import annotations
import os, json
import pandas as pd
from config import CONFIG

RISK_STATE = "reports/metrics/risk_state.json"

def _read():
    if os.path.exists(RISK_STATE):
        try: return json.load(open(RISK_STATE))
        except Exception: pass
    return {"daily_loss": 0.0, "drawdown_exceeded": False}

def _write(s):
    os.makedirs(os.path.dirname(RISK_STATE) or ".", exist_ok=True)
    json.dump(s, open(RISK_STATE, "w"), indent=2)

def apply_guardrails(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enforce hard constraints:
      - daily freeze if drawdown exceeded (paper proxy)
      - per-trade size cap
    """
    if df is None or df.empty: return df
    s = _read()
    if s.get("drawdown_exceeded"):
        return df.iloc[0:0]  # freeze new trades

    d = df.copy()
    cap = 0.20  # default 20%
    try:
        # If ALGO will go live, we may want a tighter cap; policy already sizes, this just hard-clips.
        rules = CONFIG.get("live", {}).get("algo_live_rules", {})
        cap = min(cap, float(rules.get("per_trade_cap", 0.10)))
    except Exception:
        pass

    if "size_pct" in d.columns:
        d["size_pct"] = d["size_pct"].clip(upper=cap)
    return d

def record_day_loss(pct_loss: float):
    s = _read()
    s["daily_loss"] = float(s.get("daily_loss", 0.0) + pct_loss)
    if s["daily_loss"] <= -0.03:   # -3% freeze
        s["drawdown_exceeded"] = True
    _write(s)

def reset_day():
    _write({"daily_loss": 0.0, "drawdown_exceeded": False})
