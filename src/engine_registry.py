# src/engine_registry.py
from __future__ import annotations
from typing import Callable, Dict, Any
import pandas as pd

_REGISTRY: Dict[str, Dict[str, Callable]] = {}

def register_engine(name: str,
                    train_fn: Callable[[pd.DataFrame, dict], Any],
                    predict_fn: Callable[[Any, pd.DataFrame, dict], pd.DataFrame]) -> None:
    _REGISTRY[name] = {"train": train_fn, "predict": predict_fn}

def list_engines():
    return sorted(_REGISTRY.keys())

def run_engine(name: str, train_df: pd.DataFrame, pred_df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    info = _REGISTRY.get(name)
    if not info:
        return pd.DataFrame(columns=["symbol","Score","WinProb","Reason","engine"])
    try:
        model = info["train"](train_df, cfg)
    except Exception:
        model = None
    try:
        out = info["predict"](model, pred_df, cfg)
    except Exception:
        out = pd.DataFrame(columns=["symbol","Score","WinProb","Reason"])
    if not out.empty:
        out["engine"] = name
    return out
