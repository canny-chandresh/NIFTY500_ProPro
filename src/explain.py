# src/explain.py
from __future__ import annotations
import os, json
import numpy as np
import pandas as pd
from pathlib import Path

EXPL_DIR = Path("reports/explain")
EXPL_DIR.mkdir(parents=True, exist_ok=True)

def _topk_series(s: pd.Series, k: int = 5):
    s = s.dropna()
    s = s.sort_values(ascending=False)
    return s.head(k)

def explain_linear(symbol: str, X: pd.DataFrame, coef: np.ndarray, feature_names: list[str], k: int = 5) -> str:
    imp = pd.Series(coef, index=feature_names)
    top = _topk_series(imp.abs(), k=k)
    payload = {"symbol": symbol, "model": "linear", "top_features": top.index.tolist(), "scores": top.values.tolist()}
    path = EXPL_DIR / f"{symbol}_explain.json"
    json.dump(payload, open(path,"w"), indent=2)
    return str(path)

def explain_tree(symbol: str, feature_importances_: np.ndarray, feature_names: list[str], k: int = 5) -> str:
    imp = pd.Series(feature_importances_, index=feature_names)
    top = _topk_series(imp, k=k)
    payload = {"symbol": symbol, "model": "tree", "top_features": top.index.tolist(), "scores": top.values.tolist()}
    path = EXPL_DIR / f"{symbol}_explain.json"
    json.dump(payload, open(path,"w"), indent=2)
    return str(path)

def explain_generic(symbol: str, feat_df: pd.DataFrame, pred_contribs: pd.Series | None = None, k: int = 5) -> str:
    # Fallback: use variance of features as crude importance if nothing else
    scores = feat_df.var(numeric_only=True)
    top = _topk_series(scores, k=k)
    payload = {"symbol": symbol, "model": "generic", "top_features": top.index.tolist(), "scores": top.values.tolist()}
    path = EXPL_DIR / f"{symbol}_explain.json"
    json.dump(payload, open(path,"w"), indent=2)
    return str(path)
