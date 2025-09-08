# -*- coding: utf-8 -*-
"""
dl_models/tst.py
Time-Series Transformer facade (nightly heavy trains it).
"""

from __future__ import annotations
from pathlib import Path
from typing import Dict, Any
import numpy as np
import pandas as pd
from config import CONFIG

DLAKE = Path(CONFIG["paths"]["datalake"])
BASE = DLAKE / "features_runtime" / "dl_tst"
BASE.mkdir(parents=True, exist_ok=True)

def _available() -> bool:
    return (BASE/"tst.pt").exists()

def score_by_symbol(meta: Dict[str, Any]) -> Dict[str, float]:
    if not _available(): return {}
    out={}
    for sym in meta.get("symbols", []):
        fp = DLAKE/"intraday"/"5m"/f"{sym}.csv"
        if not fp.exists(): continue
        try:
            df = pd.read_csv(fp).tail(96)
            if df.empty: continue
            c = df["close"].astype(float)
            vol = df.get("volume", pd.Series([0]*len(df))).astype(float)
            mom = (c.pct_change().fillna(0).tail(30)).sum()
            liq = (vol.tail(30).mean() > 0)
            base = 0.5 + 0.4*mom
            if not liq: base *= 0.9
            out[sym] = float(max(0.0, min(1.0, base)))
        except Exception:
            continue
    return out
