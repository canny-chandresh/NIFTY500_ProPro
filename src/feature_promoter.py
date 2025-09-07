# src/feature_promoter.py
from __future__ import annotations
import json, yaml, datetime as dt
from pathlib import Path
import pandas as pd
import numpy as np

CAT = Path("reports/auto_features/catalog.json")
SPEC = Path("config/feature_spec.yaml")
PROM = Path("config/promoted_features.yaml")

DEFAULT_RULES = {
    "min_ic": 0.02,               # minimum Information Coefficient
    "min_stability": 0.60,        # H1 vs H2 IC closeness
    "max_psi": 0.20,              # drift guard (lower is better)
    "max_per_symbol": 3,          # avoid overfitting per name
    "max_total": 30               # promote at most this many features globally
}

def _load_catalog() -> pd.DataFrame:
    if not CAT.exists(): return pd.DataFrame()
    return pd.read_json(CAT)

def _load_spec(path: Path) -> dict:
    if not path.exists(): return {"features": [], "targets": []}
    return yaml.safe_load(path.read_text())

def _save_yaml(path: Path, data: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")

def select_candidates(df: pd.DataFrame, rules=DEFAULT_RULES) -> pd.DataFrame:
    d = df.copy()
    if d.empty: return d
    # basic filters
    d = d[(d["ic"] >= rules["min_ic"]) & (d["stability"] >= rules["min_stability"])]
    d = d[(d["psi"].isna()) | (d["psi"] <= rules["max_psi"])]
    # dedupe by (feature) keep best symbol
    d = d.sort_values(["feature","ic"], ascending=[True, False]).drop_duplicates("feature")
    # cap per symbol
    d["sym_rank"] = d.groupby("symbol")["ic"].rank(ascending=False, method="first")
    d = d[d["sym_rank"] <= rules["max_per_symbol"]]
    # global cap
    d = d.sort_values("ic", ascending=False).head(rules["max_total"])
    return d.drop(columns=["sym_rank"])

def write_promotions(cands: pd.DataFrame):
    """
    We write a small sidecar YAML that your features_builder can read to merge AUTO features.
    Each promoted feature becomes a generic column name, sourced from AUTO::<feature>.
    """
    spec = _load_spec(PROM)
    cur = spec.get("auto_features", [])
    for _, r in cands.iterrows():
        cur.append({
            "name": f"AUTO_{r['feature']}",
            "source": f"AUTO::{r['feature']}",
            "notes": f"auto-promoted from {r['symbol']} ic={r['ic']:.3f} stab={r['stability']:.2f} psi={r['psi']}"
        })
    spec["auto_features"] = cur
    spec["when_utc"] = dt.datetime.utcnow().isoformat()+"Z"
    _save_yaml(PROM, spec)
    return PROM

def run_promoter():
    df = _load_catalog()
    if df.empty:
        return {"promoted": 0, "reason": "no_catalog"}
    sel = select_candidates(df)
    path = write_promotions(sel)
    return {"promoted": int(len(sel)), "promoted_file": str(path)}

if __name__ == "__main__":
    print(run_promoter())
