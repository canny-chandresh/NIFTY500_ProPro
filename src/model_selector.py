# src/model_selector.py
"""
Chooses which ML/DL model to use: light, robust, or DL shadow.
"""

import os, random, pandas as pd
from config import CONFIG
from ai_policy import apply_policy, build_context

def _dummy_picks(n=10):
    syms = [f"SYM{i}" for i in range(1, n+1)]
    return pd.DataFrame({
        "Symbol": syms,
        "Entry": [100+random.random()*10 for _ in syms],
        "Target": [105+random.random()*10 for _ in syms],
        "SL": [97+random.random()*10 for _ in syms],
        "proba": [0.5+random.random()*0.4 for _ in syms],
        "Reason": ["demo"]*n,
    })

def choose_and_predict_full(top_k: int=5):
    ctx = build_context()
    # Simplified: always use robust if enabled
    mode = CONFIG.get("model", {}).get("mode", "robust")
    raw = _dummy_picks(20)

    picks = apply_policy(raw, ctx)
    picks = picks.sort_values("proba", ascending=False).head(top_k).reset_index(drop=True)
    return picks, mode
