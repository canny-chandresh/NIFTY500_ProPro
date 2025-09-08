# -*- coding: utf-8 -*-
"""
dl_models/ft_transformer.py
Minimal scoring facade. If a trained model artifact exists, load & score;
otherwise return zeros. Nightly heavy can populate ft_transformer.pt later.
"""

from __future__ import annotations
from pathlib import Path
import numpy as np
from config import CONFIG

BASE = Path(CONFIG["paths"]["datalake"]) / "features_runtime" / "dl_ft"
BASE.mkdir(parents=True, exist_ok=True)

def _model_available() -> bool:
    return (BASE/"ft_transformer.pt").exists()

def score(X: np.ndarray) -> np.ndarray:
    if X.size == 0 or not _model_available():
        return np.zeros((X.shape[0],), dtype=float)
    # Placeholder: in real training we’d load Torch and do inference.
    # Keep a deterministic pseudo-score using last column for now.
    s = X[:, -1]
    s = (s - s.min()) / ((s.max() - s.min()) or 1.0)
    return s
