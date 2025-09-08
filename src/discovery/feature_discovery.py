# -*- coding: utf-8 -*-
"""
Search for new useful features:
- stability (missing/NaN)
- drift vs. history
- MI/permutation importance against short-horizon target proxy
Writes candidate_features.yaml and discovery_report.html
"""

from __future__ import annotations
from pathlib import Path
import yaml, json
import numpy as np
import pandas as pd
from typing import Dict, Any
from config import CONFIG

ROOT = Path(".")
DLAKE = Path(CONFIG["paths"]["datalake"])
RPT = Path(CONFIG["paths"]["reports"]) / "discovery"
RPT.mkdir(parents=True, exist_ok=True)

def _load_matrix() -> pd.DataFrame:
    p = DLAKE / "daily_hot.parquet"
    if not p.exists(): return pd.DataFrame()
    return pd.read_parquet(p)

def _basic_target_proxy(df: pd.DataFrame) -> pd.Series:
    # next-day direction proxy (for quick MI): sign(close_{t+1}-close_t)
    if df.empty: return pd.Series([], dtype=float)
    g = df.sort_values(["symbol","date"]).groupby("symbol")
    nxt = g["close"].shift(-1)
    return ((nxt - df["close"]) / df["close"].replace(0,np.nan)).fillna(0.0)

def _drift_score(s: pd.Series) -> float:
    x = s.dropna().astype(float)
    if len(x) < 50: return 0.0
    w = len(x)//2
    a, b = x.iloc[:w], x.iloc[w:]
    return float(abs(a.mean() - b.mean()) / (a.std()+1e-9))

def _mi_like(x: pd.Series, y: pd.Series) -> float:
    # crude MI proxy: abs(corr) after rank transform
    if x.empty or y.empty: return 0.0
    xr = x.rank(pct=True); yr = y.rank(pct=True)
    c = xr.corr(yr)
    return float(abs(c) if pd.notnull(c) else 0.0)

def run() -> Dict[str, Any]:
    df = _load_matrix()
    out = {"candidates": []}
    if df.empty:
        (RPT/"discovery_report.html").write_text("<h3>No data</h3>")
        return out
    y = _basic_target_proxy(df)

    # candidate transforms over base columns
    base_cols = [c for c in df.columns if c not in ("symbol","date")]
    for c in base_cols:
        s = df[c].astype(float)
        stbl = float(1.0 - s.isna().mean())
        drift = _drift_score(s)
        mi = _mi_like(s, y)
        score = 0.5*mi + 0.3*stbl - 0.2*drift
        if score > 0.05:
            out["candidates"].append({"name": f"auto_{c}_z", "base": c, "mi": mi, "stability": stbl, "drift": drift, "score": score})

    out["candidates"] = sorted(out["candidates"], key=lambda r: r["score"], reverse=True)[:CONFIG["discovery"].get("max_new_features_per_night",3)]

    # save yaml
    (DLAKE/"discovery").mkdir(parents=True, exist_ok=True)
    (DLAKE/"discovery"/"candidate_features.yaml").write_text(yaml.safe_dump(out, sort_keys=False))
    # simple HTML summary
    html = ["<h3>Discovery Candidates</h3><ol>"]
    for c in out["candidates"]:
        html.append(f"<li>{c['name']} (base={c['base']}) → score={c['score']:.3f} | mi={c['mi']:.3f} | drift={c['drift']:.3f}</li>")
    html.append("</ol>")
    (RPT/"discovery_report.html").write_text("\n".join(html))
    return out
