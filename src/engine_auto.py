# src/engine_auto.py
from __future__ import annotations
import numpy as np, pandas as pd
from typing import Dict, Any
from engine_registry import register_engine
from _engine_utils import last_per_symbol

NAME = "AUTO_TOPK"

def train(df: pd.DataFrame, cfg: Dict) -> Any:
    return {"ok": True}

def predict(model: Any, pred_df: pd.DataFrame, cfg: Dict) -> pd.DataFrame:
    P = last_per_symbol(pred_df)
    if P.empty:
        return pd.DataFrame(columns=["symbol","Score","WinProb","Reason"])
    # Simple allocator: momentum + sector momentum if available
    score = np.zeros(len(P))
    if "MAN_ret1" in P: score += P["MAN_ret1"].fillna(0).to_numpy()
    if "SECTOR_mom" in P: score += 0.5 * P["SECTOR_mom"].fillna(0).to_numpy()
    out = P[["symbol"]].copy()
    out["Score"] = score
    out["WinProb"] = (0.5 + np.tanh((score - np.mean(score))/(np.std(score)+1e-9))/4.0).clip(0.1,0.9)
    out["Reason"] = "Top-K allocator"
    return out

register_engine(NAME, train, predict)
