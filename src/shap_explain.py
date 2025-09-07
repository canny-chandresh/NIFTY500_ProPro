# src/shap_explain.py
from __future__ import annotations
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

# Optional heavy deps; import guarded
def _opt_import(name: str):
    try:
        return __import__(name)
    except Exception:
        return None

_shap = _opt_import("shap")
_skl = _opt_import("sklearn")
_xgb = _opt_import("xgboost")
plt = None
try:
    import matplotlib.pyplot as _plt
    plt = _plt
except Exception:
    plt = None

ART = Path("reports/explain"); ART.mkdir(parents=True, exist_ok=True)

def _topk(d: Dict[str, float], k: int = 12) -> Dict[str, float]:
    return dict(sorted(d.items(), key=lambda kv: abs(kv[1]), reverse=True)[:k])

def explain_tree_model(model: Any, X: pd.DataFrame, topk_n: int = 12,
                       tag: str = "ml") -> Dict:
    """
    Tries SHAP TreeExplainer for tree models; falls back to permutation importance.
    Returns { importances, png_path, json_path }.
    """
    if X is None or len(X) == 0:
        return {"ok": False, "reason": "empty_X"}

    importances: Dict[str, float] = {}

    # Try SHAP first
    if _shap is not None and model is not None:
        try:
            explainer = _shap.TreeExplainer(model)
            # sample to keep CI fast
            sample = X.sample(min(800, len(X)), random_state=42)
            vals = explainer.shap_values(sample)
            # shap_values shape: (rows, features) or list for multiclass
            if isinstance(vals, list):
                # pick mean absolute across classes
                v = np.mean([np.abs(vv).mean(axis=0) for vv in vals], axis=0)
            else:
                v = np.abs(vals).mean(axis=0)
            importances = {c: float(v[i]) for i, c in enumerate(sample.columns)}
        except Exception:
            importances = {}

    # Fallback: permutation importance (sklearn)
    if not importances and _skl is not None:
        try:
            from sklearn.inspection import permutation_importance
            sample = X.sample(min(1200, len(X)), random_state=42)
            # Need a scorer; use model.predict_proba if exists else predict
            if hasattr(model, "predict_proba"):
                yhat = model.predict_proba(sample)[:, 1]
            else:
                yhat = model.predict(sample)
            # Build pseudo-target from predicted proba to measure sensitivity
            y = (yhat > np.median(yhat)).astype(int)
            r = permutation_importance(model, sample, y, n_repeats=5, random_state=42)
            importances = {c: float(r.importances_mean[i]) for i, c in enumerate(sample.columns)}
        except Exception:
            importances = {}

    if not importances:
        return {"ok": False, "reason": "no_importances"}

    top = _topk(importances, k=topk_n)

    # Save JSON
    jpath = ART / f"feature_importance_{tag}.json"
    jpath.write_text(json.dumps({"topk": top, "n_features": len(importances)}, indent=2), encoding="utf-8")

    # Save simple bar chart
    ppath = None
    if plt is not None:
        try:
            labels = list(top.keys())
            vals = [top[k] for k in labels]
            plt.figure(figsize=(8, 4.5))
            plt.barh(labels[::-1], vals[::-1])
            plt.title(f"Top-{topk_n} feature importances ({tag})")
            plt.tight_layout()
            ppath = ART / f"feature_importance_{tag}.png"
            plt.savefig(ppath, dpi=140)
            plt.close()
        except Exception:
            ppath = None

    return {"ok": True, "json": str(jpath), "png": (str(ppath) if ppath else None), "topk": top}
