# -*- coding: utf-8 -*-
"""
engines/calibration.py
Probability calibration stubs. In this minimal version we expose a 'calibrate' that
linearly maps a 0..1 score to a probability and optionally shrinks toward 0.5.
Nightly can later fit Platt/Isotonic and save params to datalake/features_runtime/calibration/.
"""

from __future__ import annotations
from pathlib import Path
from typing import Dict, Any
import json

from config import CONFIG

BASE = Path(CONFIG["paths"]["datalake"]) / "features_runtime" / "calibration"
BASE.mkdir(parents=True, exist_ok=True)

def _load_params() -> Dict[str, Any]:
    p = BASE / "platt.json"
    if p.exists():
        try: return json.loads(p.read_text())
        except Exception: pass
    return {"a": 1.0, "b": 0.0, "sigma": 0.15}  # simple default

def calibrate(raw_score: float) -> tuple[float, float]:
    """
    Returns (prob, sigma). Sigma used by AI tempering.
    """
    prm = _load_params()
    a = float(prm.get("a", 1.0)); b = float(prm.get("b", 0.0))
    x = max(0.0, min(1.0, raw_score))
    p = max(0.0, min(1.0, a * x + b))
    sigma = float(prm.get("sigma", 0.15))
    return p, sigma
