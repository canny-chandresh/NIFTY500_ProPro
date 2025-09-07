# src/feature_promoter.py
"""
Promote candidate features to AUTO_* with caps:
- p_value <= 0.01
- stable sign over N windows
- max promotions/week cap
Writes: datalake/features_auto/<symbol>_auto.csv and config/promoted_features.yaml
"""
from __future__ import annotations
from pathlib import Path
import pandas as pd, numpy as np, yaml, datetime as dt

CAND = Path("datalake/auto_candidates")
AUTO = Path("datalake/features_auto"); AUTO.mkdir(parents=True, exist_ok=True)
PROM = Path("config/promoted_features.yaml"); PROM.parent.mkdir(parents=True, exist_ok=True)

def _tstat(x: pd.Series, y: pd.Series) -> float:
    # simple correlation->t heuristic
    x, y = x.dropna(), y.dropna()
    n = min(len(x), len(y))
    if n < 60: return 0.0
    r = pd.concat([x, y], axis=1).corr().iloc[0,1]
    if pd.isna(r): return 0.0
    return float(r * np.sqrt((n-2)/(1-r**2 + 1e-9)))

def run_promoter(max_weekly_promotions: int = 10, stability_windows: int = 6) -> dict:
    if not CAND.exists(): return {"ok": False, "reason": "no_candidates"}
    promoted = {"auto_features": []}
    if PROM.exists():
        promoted = yaml.safe_load(PROM.read_text()) or promoted

    added = 0
    for p in sorted(CAND.glob("*_candidates.csv")):
        sym = p.stem.replace("_candidates","")
        df = pd.read_csv(p, parse_dates=["Date"])
        if "y_1d" not in df.columns: continue
        tgt = df["y_1d"]
        # examine candidate columns prefixed CAND_
        cols = [c for c in df.columns if c.startswith("CAND_")]
        keep = []
        for c in cols:
            t = _tstat(df[c], tgt)
            # p-value approx threshold |t| > 2.58 (~p<=0.01)
            if abs(t) < 2.58: continue
            # stability: sign of rolling corr stays same across windows
            win = max(40, len(df)//stability_windows)
            signs = []
            for i in range(stability_windows):
                sl = df.iloc[i*win:(i+1)*win]
                if len(sl) < 30: continue
                r = sl[c].corr(sl["y_1d"])
                signs.append(np.sign(r if not pd.isna(r) else 0))
            if len(signs) >= max(3, stability_windows-2) and (abs(sum(signs)) >= len(signs)-1):
                keep.append(c)
        if not keep: continue
        # cap promotions per week
        quota = max_weekly_promotions - added
        if quota <= 0: break
        sel = keep[:quota]
        # write/merge AUTO file
        out = df[["Date"] + sel].copy()
        out.columns = ["Date"] + [f"AUTO_{c[5:]}" for c in sel]
        auto_path = AUTO / f"{sym}_auto.csv"
        try:
            prev = pd.read_csv(auto_path, parse_dates=["Date"])
            out = prev.merge(out, on="Date", how="outer")
        except Exception:
            pass
        out.to_csv(auto_path, index=False)
        # update catalog
        for c in sel:
            promoted["auto_features"].append({"source": f"AUTO::{c[5:]}", "symbol": sym, "added": dt.datetime.utcnow().isoformat()+"Z"})
        added += len(sel)

    PROM.write_text(yaml.safe_dump(promoted, sort_keys=False), encoding="utf-8")
    return {"ok": True, "added": added}
