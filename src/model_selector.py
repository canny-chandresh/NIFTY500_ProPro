# src/model_selector.py
from __future__ import annotations
import importlib
import pandas as pd
from typing import Dict

# Ensure engines are imported so they self-register
_ENGINE_MODULES = [
    "engine_ml_robust",
    "engine_algo_rules",
    "engine_auto",
    "engine_ufd",
    "engine_dl_temporal",
    "engine_dl_transformer",
    "engine_gnn",
    "engine_lstm",
]

for m in _ENGINE_MODULES:
    try:
        importlib.import_module(m)
    except Exception as e:
        print(f"[model_selector] optional engine '{m}' not loaded: {e}")

from engine_registry import run_engine

def run_engines(train_df: pd.DataFrame, pred_df: pd.DataFrame, cfg: Dict) -> pd.DataFrame:
    active = cfg.get("engines_active") or []
    frames = []
    for name in active:
        try:
            df = run_engine(name, train_df, pred_df, cfg)
            if not df.empty:
                frames.append(df)
        except Exception as e:
            print(f"[model_selector] engine {name} failed: {e}")
    if not frames:
        return pd.DataFrame(columns=["symbol","Score","WinProb","Reason","engine"])
    return pd.concat(frames, ignore_index=True)
