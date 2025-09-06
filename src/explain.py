# src/explain.py
from __future__ import annotations
import os, json
from pathlib import Path
import numpy as np
import pandas as pd

EXPL_DIR = Path("reports/explain")
EXPL_DIR.mkdir(parents=True, exist_ok=True)

def _write_index():
    items = sorted(EXPL_DIR.glob("*_shap.html"))
    rows = "\n".join([f'<li><a href="{p.name}">{p.name}</a></li>' for p in items])
    html = f"""<!doctype html><meta charset="utf-8">
    <h3>Explainability Reports</h3><ul>{rows}</ul>"""
    (EXPL_DIR / "index.html").write_text(html, encoding="utf-8")

def run_explain_tree(symbol: str, model, X: pd.DataFrame, feature_names: list[str], top_k: int = 8) -> dict:
    out = {"symbol": symbol, "html": None, "json": None, "method": None}
    try:
        import shap
        explainer = shap.TreeExplainer(model)
        shap_vals = explainer.shap_values(X)
        shap_html = shap.force_plot(explainer.expected_value, shap_vals[:top_k], X.iloc[:top_k,:], matplotlib=False)
        html_path = EXPL_DIR / f"{symbol}_shap.html"
        shap.save_html(str(html_path), shap_html)
        out["html"] = str(html_path); out["method"] = "shap"
    except Exception:
        try:
            from sklearn.inspection import permutation_importance
            r = permutation_importance(model, X, model.predict(X), n_repeats=4, random_state=42)
            imp = pd.Series(r.importances_mean, index=feature_names).sort_values(ascending=False).head(top_k)
            payload = {"symbol": symbol,"method":"permutation","top_features": imp.index.tolist(),"scores":imp.values.tolist()}
            jpath = EXPL_DIR / f"{symbol}_explain.json"
            json.dump(payload, open(jpath,"w"), indent=2)
            out["json"] = str(jpath); out["method"] = "permutation"
        except Exception:
            var = X.var(numeric_only=True).sort_values(ascending=False).head(top_k)
            payload = {"symbol": symbol,"method":"variance","top_features": var.index.tolist(),"scores":var.values.tolist()}
            jpath = EXPL_DIR / f"{symbol}_explain.json"
            json.dump(payload, open(jpath,"w"), indent=2)
            out["json"] = str(jpath); out["method"] = "variance"
    _write_index()
    return out
