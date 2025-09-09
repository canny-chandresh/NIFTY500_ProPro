# -*- coding: utf-8 -*-
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

def _load_matrix(horizon_days: int) -> pd.DataFrame:
    p = DLAKE / "daily_hot.parquet"
    if not p.exists():
        return pd.DataFrame()
    df = pd.read_parquet(p)
    # limit horizon by date (approx)
    if "date" in df.columns:
        df = df.sort_values("date").tail(horizon_days * len(df["symbol"].unique()))
    return df

def _basic_target_proxy(df: pd.DataFrame, forward_days=2) -> pd.Series:
    if df.empty: return pd.Series([], dtype=float)
    g = df.sort_values(["symbol","date"]).groupby("symbol", group_keys=False)
    nxt = g["close"].shift(-forward_days)
    return ((nxt - df["close"]) / df["close"].replace(0,np.nan)).fillna(0.0)

def _drift_score(s: pd.Series) -> float:
    x = s.dropna().astype(float)
    if len(x) < 60: return 0.0
    w = len(x)//2
    a, b = x.iloc[:w], x.iloc[w:]
    return float(abs(a.mean() - b.mean()) / (a.std()+1e-9))

def _mi_like(x: pd.Series, y: pd.Series) -> float:
    if x.empty or y.empty: return 0.0
    xr = x.rank(pct=True); yr = y.rank(pct=True)
    c = xr.corr(yr)
    return float(abs(c) if pd.notnull(c) else 0.0)

def run() -> Dict[str, Any]:
    weekly = (RPT/"_weekly.flag").exists()
    horizon_days = 250 if weekly else 125  # ~6M vs ~3M
    fwd = 3 if weekly else 2

    df = _load_matrix(horizon_days=horizon_days)
    out = {"candidates": [], "weekly": weekly}
    if df.empty:
        (RPT/"discovery_report.html").write_text("<h3>No data</h3>")
        return out

    y = _basic_target_proxy(df, forward_days=fwd)

    base_cols = [c for c in df.columns if c not in ("symbol","date")]
    for c in base_cols:
        s = df[c].astype(float)
        stbl = float(1.0 - s.isna().mean())
        drift = _drift_score(s)
        mi = _mi_like(s, y)
        score = 0.55*mi + 0.35*stbl - 0.20*drift
        if score > 0.06:
            out["candidates"].append({"name": f"auto_{c}_z", "base": c, "mi": mi, "stability": stbl, "drift": drift, "score": score})

    out["candidates"] = sorted(out["candidates"], key=lambda r: r["score"], reverse=True)[:CONFIG["discovery"].get("max_new_features_per_night",5)]

    (DLAKE/"discovery").mkdir(parents=True, exist_ok=True)
    (DLAKE/"discovery"/"candidate_features.yaml").write_text(yaml.safe_dump(out, sort_keys=False))

    html = [f"<h3>Discovery Candidates ({'Weekly' if weekly else 'Nightly'})</h3><ol>"]
    for c in out["candidates"]:
        html.append(f"<li>{c['name']} (base={c['base']}) → score={c['score']:.3f} | mi={c['mi']:.3f} | drift={c['drift']:.3f}</li>")
    html.append("</ol>")
    (RPT/"discovery_report.html").write_text("\n".join(html))
    return out
