# src/engine_ml_robust.py
from __future__ import annotations
import numpy as np, pandas as pd
from typing import Any, Dict
from engine_registry import register_engine
from _engine_utils import feature_cols, last_per_symbol, safe_winprob

# Try sklearn; fallback to a simple logistic score
try:
    from sklearn.ensemble import RandomForestClassifier
    SKLEARN_OK = True
except Exception:
    SKLEARN_OK = False

NAME = "ML_ROBUST"

def _prep(df: pd.DataFrame):
    X = df[feature_cols(df)].replace([np.inf,-np.inf], np.nan).fillna(0.0)
    y = (df.get("y_1d", pd.Series(0, index=df.index)) > 0).astype(int)
    return X, y

def train(df: pd.DataFrame, cfg: Dict) -> Any:
    if df is None or df.empty: return None
    if SKLEARN_OK:
        X, y = _prep(df)
        if y.sum() == 0 or y.sum() == len(y):  # degenerate
            return {"degenerate": True, "p": float(y.mean())}
        clf = RandomForestClassifier(n_estimators=150, max_depth=6, random_state=42, n_jobs=-1)
        clf.fit(X, y)
        return clf
    # fallback dummy model
    p = float((df.get("y_1d", pd.Series(0)) > 0).mean())
    return {"degenerate": True, "p": p}

def predict(model: Any, pred_df: pd.DataFrame, cfg: Dict) -> pd.DataFrame:
    P = last_per_symbol(pred_df)
    if P.empty:
        return pd.DataFrame(columns=["symbol","Score","WinProb","Reason"])
    X = P[feature_cols(P)].replace([np.inf,-np.inf], np.nan).fillna(0.0)
    if model is None:
        win = 0.5
        score = X.mean(axis=1).to_numpy()
    elif isinstance(model, dict) and model.get("degenerate"):
        win = model.get("p", 0.5)
        score = X.mean(axis=1).to_numpy()
    else:
        try:
            prob = model.predict_proba(X)[:,1]
        except Exception:
            prob = np.clip((X.mean(axis=1).to_numpy() - X.mean().mean())/ (X.std().mean()+1e-9) * 0.1 + 0.5, 0.05, 0.95)
        win = prob
        score = prob
    out = P[["symbol"]].copy()
    out["Score"] = np.asarray(score, dtype=float)
    out["WinProb"] = np.asarray(win, dtype=float).clip(0.05,0.95)
    out["Reason"] = "RF(robust)" if SKLEARN_OK else "RF(fallback)"
    return out

register_engine(NAME, train, predict)
