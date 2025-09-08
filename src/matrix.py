# -*- coding: utf-8 -*-
"""
matrix.py
Transforms feature frame -> X matrix with stable column order enforced by feature_spec.yaml
Also returns metadata used by symbol-level engines (TCN/TST).
"""

from __future__ import annotations
from pathlib import Path
from typing import Tuple, Dict, Any, List
import yaml
import numpy as np
import pandas as pd
from config import CONFIG

SPEC_FILE = Path(CONFIG["feature_spec_file"])

_DEFAULT_SPEC = {
    "keep": [
        "ema20","ema50","atr_pct","pivot","gap_pct","close_in_gap","india_vix",
        "close","volume"
    ],
    "target": None,
    "scaling": "standard"
}

def _save_spec(spec: dict):
    SPEC_FILE.parent.mkdir(parents=True, exist_ok=True)
    SPEC_FILE.write_text(yaml.safe_dump(spec, sort_keys=False))

def _load_spec() -> dict:
    if not SPEC_FILE.exists():
        _save_spec(_DEFAULT_SPEC)
        return _DEFAULT_SPEC
    try:
        return yaml.safe_load(SPEC_FILE.read_text()) or _DEFAULT_SPEC
    except Exception:
        return _DEFAULT_SPEC

def build_matrix(ff: pd.DataFrame) -> Tuple[np.ndarray, List[str], Dict[str,Any], pd.DataFrame]:
    if ff is None or ff.empty:
        return np.zeros((0,0), dtype=float), [], {"symbols":[]}, ff
    spec = _load_spec()
    cols = [c for c in spec.get("keep", []) if c in ff.columns]
    # minimally, ensure close/atr_pct exist
    for c in ["close","atr_pct"]:
        if c not in cols and c in ff.columns: cols.append(c)

    X = ff[cols].astype(float).replace([np.inf, -np.inf, np.nan], 0.0).to_numpy()
    meta = {"symbols": ff["symbol"].tolist()}
    return X, cols, meta, ff
