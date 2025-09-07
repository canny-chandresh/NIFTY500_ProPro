# src/engine_gnn.py
from __future__ import annotations
import numpy as np, pandas as pd
from typing import Dict, Any
from engine_registry import register_engine
from _engine_utils import last_per_symbol

NAME = "DL_GNN"

def train(df: pd.DataFrame, cfg: Dict) -> Any:
    # learn simple weights for GRAPH_* to target
    cols = [c for c in df.columns if c.startswith("GRAPH_")]
    if not cols:
        return {"cols": [], "w": None}
    X = df[cols].fillna(0.0).to_numpy()
    y = (df.get("y_1d", pd.Series(0, index=df.index)) > 0).astype(int).to_numpy()
    if y.sum() == 0 or y.sum() == len(y):
        w = np.ones(X.shape[1]) / max(1, X.shape[1])
    else:
        w = []
        for i, c in enumerate(cols):
            try:
                r = np.corrcoef(X[:,i], y)[0,1]
            except Exception:
                r = 0.0
            w.append(r)
        w = np.array(w)
        if np.allclose(w, 0): w = np.ones_like(w)
        w = w / (np.linalg.norm(w)+1e-9)
    return {"cols": cols, "w": w}

def predict(model: Any, pred_df: pd.DataFrame, cfg: Dict) -> pd.DataFrame:
    P = last_per_symbol(pred_df)
    if P.empty:
        return pd.DataFrame(columns=["symbol","Score","WinProb","Reason"])
    cols = model.get("cols", []) if isinstance(model, dict) else []
    if not cols:
        z = P.get("GRAPH_deg", pd.Series(0, index=P.index)).fillna(0.0).to_numpy()
    else:
        X = P[cols].fillna(0.0).to_numpy()
        w = model.get("w")
        z = X.dot(w) if w is not None else X.sum(axis=1)
    out = P[["symbol"]].copy()
    out["Score"] = z
    out["WinProb"] = (0.5 + np.tanh(z)/4.0).clip(0.1,0.9)
    out["Reason"] = "Graph centrality (shadow)"
    return out

register_engine(NAME, train, predict)
