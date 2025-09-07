# src/engine_lstm.py
from __future__ import annotations
import numpy as np, pandas as pd
from typing import Dict, Any
from engine_registry import register_engine
from _engine_utils import last_per_symbol

NAME = "DL_LSTM"

# Torch is optional; we still return a safe proxy if not available
try:
    import torch
    TORCH_OK = True
except Exception:
    TORCH_OK = False

def _seq_proxy(df: pd.DataFrame) -> pd.Series:
    # Simple sequential proxy: 10d EMA slope if available
    if "MAN_ret1" in df.columns:
        return df.groupby("symbol")["MAN_ret1"].transform(lambda s: s.rolling(10, min_periods=3).mean())
    return pd.Series(0.0, index=df.index)

def train(df: pd.DataFrame, cfg: Dict) -> Any:
    if not TORCH_OK or df.empty:
        sig = _seq_proxy(df).fillna(0.0)
        return {"mu": float(sig.mean()), "sd": float(sig.std()+1e-9), "torch": False}
    # Keep it minimal for CI (no heavy training)
    sig = _seq_proxy(df).fillna(0.0)
    return {"mu": float(sig.mean()), "sd": float(sig.std()+1e-9), "torch": True}

def predict(model: Any, pred_df: pd.DataFrame, cfg: Dict) -> pd.DataFrame:
    P = last_per_symbol(pred_df)
    if P.empty:
        return pd.DataFrame(columns=["symbol","Score","WinProb","Reason"])
    sig = _seq_proxy(P).fillna(0.0)
    mu = model.get("mu", 0.0) if isinstance(model, dict) else 0.0
    sd = model.get("sd", 1.0) if isinstance(model, dict) else 1.0
    z = (sig - mu) / (sd+1e-9)
    out = P[["symbol"]].copy()
    out["Score"] = z.replace([np.inf,-np.inf], 0.0).to_numpy()
    out["WinProb"] = (1/(1+np.exp(-z))).clip(0.1,0.9).to_numpy()
    out["Reason"] = "LSTM baseline (proxy)" if TORCH_OK else "LSTM fallback (proxy)"
    return out

register_engine(NAME, train, predict)
