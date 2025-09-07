# src/engine_ufd.py
from __future__ import annotations
import numpy as np, pandas as pd
from typing import Dict, Any
from engine_registry import register_engine
from _engine_utils import last_per_symbol

NAME = "UFD_PROMOTED"

def _auto_cols(df: pd.DataFrame):
    return [c for c in df.columns if c.startswith("AUTO_")]

def train(df: pd.DataFrame, cfg: Dict) -> Any:
    cols = _auto_cols(df)
    if not cols: return {"cols": [], "w": None}
    X = df[cols].fillna(0.0).to_numpy()
    y = (df.get("y_1d", pd.Series(0, index=df.index)) > 0).astype(int).to_numpy()
    if y.sum() == 0 or y.sum() == len(y):
        w = np.ones(X.shape[1]) / max(1, X.shape[1])
    else:
        # simple correlation weights
        w = []
        for i, c in enumerate(cols):
            try:
                r = np.corrcoef(X[:,i], y)[0,1]
            except Exception:
                r = 0.0
            w.append(r)
        w = np.array(w)
        if np.allclose(w, 0): w = np.ones_like(w)
        w = w / (np.linalg.norm(w) + 1e-9)
    return {"cols": cols, "w": w}

def predict(model: Any, pred_df: pd.DataFrame, cfg: Dict) -> pd.DataFrame:
    P = last_per_symbol(pred_df)
    if P.empty:
        return pd.DataFrame(columns=["symbol","Score","WinProb","Reason"])
    cols = model.get("cols", []) if isinstance(model, dict) else []
    if not cols:
        sc = P.filter(like="AUTO_").sum(axis=1).fillna(0.0).to_numpy()
    else:
        X = P[cols].fillna(0.0).to_numpy()
        w = model.get("w")
        sc = X.dot(w) if w is not None else X.sum(axis=1)
    out = P[["symbol"]].copy()
    out["Score"] = sc
    out["WinProb"] = (0.5 + np.tanh((sc - np.mean(sc))/(np.std(sc)+1e-9))/4.0).clip(0.1,0.9)
    out["Reason"] = "AUTO_* discovery"
    return out

register_engine(NAME, train, predict)
