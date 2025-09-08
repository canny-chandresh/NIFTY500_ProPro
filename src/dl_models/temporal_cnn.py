# -*- coding: utf-8 -*-
"""
dl_models/temporal_cnn.py
Symbol-level scorer for intraday sequences. Returns dict symbol->score.
"""

from __future__ import annotations
from pathlib import Path
from typing import Dict, Any
import pandas as pd
import numpy as np
from config import CONFIG

DLAKE = Path(CONFIG["paths"]["datalake"])
BASE = DLAKE / "features_runtime" / "dl_tcn"
BASE.mkdir(parents=True, exist_ok=True)

def _available() -> bool:
    return (BASE/"tcn.pt").exists()

def score_by_symbol(meta: Dict[str, Any]) -> Dict[str, float]:
    if not _available():
        return {}
    out={}
    for sym in meta.get("symbols", []):
        fp = DLAKE/"intraday"/"5m"/f"{sym}.csv"
        if not fp.exists():
            continue
        try:
            df = pd.read_csv(fp).tail(96)  # ~8h at 5m
            if df.empty: continue
            # simple momentum proxy on close
            c = df["close"].astype(float)
            v = float((c.iloc[-1] - c.iloc[0]) / (abs(c.iloc[0]) or 1.0))
            out[sym] = max(0.0, min(1.0, 0.5 + 0.5*v))
        except Exception:
            continue
    return out
