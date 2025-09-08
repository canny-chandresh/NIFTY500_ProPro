# -*- coding: utf-8 -*-
"""
model_selector.py
Regime-aware routing for engines. Provides a thin abstraction so pipeline_ai
can call a common interface regardless of which engines are available.
"""

from __future__ import annotations
from typing import Dict, Any, List, Tuple
import numpy as np

# Optional engines (import lazily, always safe)
def _try_import(name: str):
    try:
        return __import__(name, fromlist=["*"])
    except Exception:
        return None

def score_ml_light(X: np.ndarray) -> np.ndarray:
    """
    Lightweight ML fallback (no external models). Returns z-score of the last column
    as a crude ranker when nothing else is available.
    """
    if X.size == 0:
        return np.zeros((0,), dtype=float)
    s = X[:, -1]  # last feature
    s = (s - s.mean()) / (s.std() + 1e-9)
    return s

def score_boosters(X: np.ndarray) -> np.ndarray:
    """
    If real booster models exist (Phase-2), call them; else fallback to ml_light.
    """
    boosters = _try_import("engines_boosters") or _try_import("engines.boosters")
    if boosters and hasattr(boosters, "score"):
        try:
            return boosters.score(X)
        except Exception:
            pass
    return score_ml_light(X)

def score_dl_ft(X: np.ndarray) -> np.ndarray:
    ft = _try_import("dl_models.ft_transformer")
    if ft and hasattr(ft, "score"):
        try: return ft.score(X)
        except Exception: pass
    return np.zeros((X.shape[0],), dtype=float)

def score_dl_tcn(meta: Dict[str, Any]) -> Dict[str, float]:
    """
    Sequence DL over intraday bars usually returns a dict symbol->score.
    If missing, return empty dict.
    """
    tcn = _try_import("dl_models.temporal_cnn")
    if tcn and hasattr(tcn, "score_by_symbol"):
        try: return tcn.score_by_symbol(meta)
        except Exception: pass
    return {}

def score_dl_tst(meta: Dict[str, Any]) -> Dict[str, float]:
    tst = _try_import("dl_models.tst")
    if tst and hasattr(tst, "score_by_symbol"):
        try: return tst.score_by_symbol(meta)
        except Exception: pass
    return {}

def blend_scores(symbols: List[str],
                 s_ml: np.ndarray,
                 s_boost: np.ndarray,
                 s_ft: np.ndarray,
                 s_tcn: Dict[str, float],
                 s_tst: Dict[str, float]) -> Dict[str, float]:
    """
    Simple normalized blend (Phase-1). Phase-2 will replace with meta-stacker + calibration.
    """
    out = {}
    mins = []; maxs = []
    cols = [s_ml, s_boost, s_ft]
    for c in cols:
        if c.size:
            mins.append(c.min()); maxs.append(c.max())
    lo = min(mins) if mins else -1.0
    hi = max(maxs) if maxs else 1.0
    rng = (hi - lo) or 1.0

    for i, sym in enumerate(symbols):
        v = 0.0; w = 0.0
        def norm(x): return (float(x) - lo) / rng
        if s_ml.size:     v += 0.2 * norm(s_ml[i]);     w += 0.2
        if s_boost.size:  v += 0.5 * norm(s_boost[i]);  w += 0.5
        if s_ft.size:     v += 0.3 * norm(s_ft[i]);     w += 0.3
        # add symbol-level sequence engines
        if sym in s_tcn:  v += 0.2 * s_tcn[sym];        w += 0.2
        if sym in s_tst:  v += 0.3 * s_tst[sym];        w += 0.3
        out[sym] = v / (w or 1.0)
    return out
