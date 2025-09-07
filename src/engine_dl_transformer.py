# src/engine_dl_transformer.py
from __future__ import annotations
import numpy as np, pandas as pd
from typing import Dict, Any
from engine_registry import register_engine
from _engine_utils import last_per_symbol

NAME = "DL_TRANSFORMER"

def train(df: pd.DataFrame, cfg: Dict) -> Any:
    # placeholder model: keep per-symbol momentum mean
    g = df.sort_values(["symbol","Date"]).groupby("symbol")
    mom = g["MAN_ret1"].apply(lambda s: s.rolling(7, min_periods=3).mean()).fillna(0.0)
    return {"global_mu": float(mom.mean()), "global_sd": float(mom.std()+1e-9)}

def predict(model: Any, pred_df: pd.DataFrame, cfg: Dict) -> pd.DataFrame:
    P = last_per_symbol(pred_df)
    if P.empty:
        return pd.DataFrame(columns=["symbol","Score","WinProb","Reason"])
    # shadow: use recent 7d momentum as proxy “transformer score”
    score = P.get("MAN_ret1", pd.Series(0, index=P.index)).fillna(0.0).to_numpy()
    mu, sd = model.get("global_mu", 0.0), model.get("global_sd", 1.0) if isinstance(model, dict) else (0.0,1.0)
    z = (score - mu) / (sd+1e-9)
    out = P[["symbol"]].copy()
    out["Score"] = z
    out["WinProb"] = (0.5 + np.tanh(z)/4.0).clip(0.1,0.9)
    out["Reason"] = "Transformer shadow (proxy)"
    return out

register_engine(NAME, train, predict)
