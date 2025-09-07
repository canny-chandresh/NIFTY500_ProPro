# src/engine_algo_rules.py
from __future__ import annotations
import numpy as np, pandas as pd
from typing import Dict, Any
from engine_registry import register_engine
from _engine_utils import last_per_symbol, safe_winprob

NAME = "ALGO_RULES"

def _score_row(r: pd.Series) -> float:
    s = 0.0
    # Examples of manual features your builder creates (rename if needed)
    if "MAN_ema20slope" in r: s += float(r["MAN_ema20slope"]) * 2.0
    if "MAN_atr14" in r and "MAN_vol20" in r:
        s += float(r["MAN_vol20"]) / (float(r["MAN_atr14"])+1e-6)
    if "MAN_gap_up" in r and r["MAN_gap_up"] > 0: s += 0.5
    if "MAN_gap_down" in r and r["MAN_gap_down"] > 0: s -= 0.5
    if "regime_flag" in r: s += 0.2 if r["regime_flag"] == 1 else (-0.1 if r["regime_flag"] == -1 else 0.0)
    return s

def train(df: pd.DataFrame, cfg: Dict) -> Any:
    return {"trained": True}

def predict(model: Any, pred_df: pd.DataFrame, cfg: Dict) -> pd.DataFrame:
    P = last_per_symbol(pred_df)
    if P.empty:
        return pd.DataFrame(columns=["symbol","Score","WinProb","Reason"])
    out = P[["symbol"]].copy()
    out["Score"] = P.apply(_score_row, axis=1)
    # Map score to winprob via logistic-ish squashing
    z = (out["Score"] - out["Score"].mean()) / (out["Score"].std()+1e-9)
    out["WinProb"] = (1/(1+np.exp(-z))).clip(0.1,0.9)
    out["Reason"] = "EMA/ATR/gap rules"
    return out

register_engine(NAME, train, predict)
