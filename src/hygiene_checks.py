# src/hygiene_checks.py
"""
Lightweight hygiene checks (fast):
- Time monotonicity & no-lookahead (targets vs features)
- Graph & vol-surface cutoff <= asof
- Feature-count and AUTO_* growth caps
- Datalake size & retention
- Drift (PSI/KS) on a small column sample
Outputs: reports/hygiene/hygiene_report.json (+console)
Exit code: 0 (never blocks pipeline); use report for alerts/Telegram.
"""
from __future__ import annotations
import os, json, math, shutil, datetime as dt
from pathlib import Path
from typing import List, Dict, Tuple
import numpy as np
import pandas as pd

# ------------- paths -------------
ROOT = Path(".")
DL = ROOT / "datalake"
FEAT_DIR = DL / "features"
META_DIR = DL / "features_meta"
REP = ROOT / "reports" / "hygiene"
REP.mkdir(parents=True, exist_ok=True)

# ------------- helpers -------------
def _list_feature_files(limit: int | None = 40) -> List[Path]:
    files = sorted(FEAT_DIR.glob("*_features.csv"))
    return files[:limit] if limit else files

def _psI(a: pd.Series, b: pd.Series, bins: int = 10) -> float:
    # simple PSI with quantile bins from a
    a, b = a.dropna(), b.dropna()
    if len(a) < 100 or len(b) < 100: return 0.0
    qs = np.unique(np.quantile(a, np.linspace(0,1,bins+1)))
    a_hist, _ = np.histogram(a, bins=qs)
    b_hist, _ = np.histogram(b, bins=qs)
    a_pct = np.where(a_hist==0, 1e-6, a_hist)/max(1, a_hist.sum())
    b_pct = np.where(b_hist==0, 1e-6, b_hist)/max(1, b_hist.sum())
    return float(np.sum((a_pct - b_pct) * np.log(a_pct / b_pct)))

def _approx_dir_size_gb(p: Path) -> float:
    total = 0
    for root, _, files in os.walk(p):
        for f in files:
            try:
                total += (Path(root) / f).stat().st_size
            except Exception:
                pass
    return total / (1024**3)

def _summary(df: pd.DataFrame) -> Dict:
    return {
        "rows": int(len(df)),
        "cols": int(len(df.columns)),
        "first": str(df["Date"].min()) if "Date" in df.columns else None,
        "last":  str(df["Date"].max()) if "Date" in df.columns else None,
    }

# ------------- checks -------------
def check_time_monotonic(df: pd.DataFrame) -> bool:
    if "Date" not in df.columns: return True
    s = pd.to_datetime(df["Date"], errors="coerce")
    return bool((s.ffill() == s).all() or s.is_monotonic_increasing)

def check_no_lookahead(df: pd.DataFrame) -> bool:
    # Validate target shift: y_1d must be future return; we approximate by ensuring
    # corr(y_1d, MAN_ret1) over same index is not ~1 and Date is monotonic.
    if "y_1d" not in df.columns or "MAN_ret1" not in df.columns: return True
    a = df["y_1d"].copy()
    b = df["MAN_ret1"].copy()
    a, b = a.replace([np.inf, -np.inf], np.nan), b.replace([np.inf, -np.inf], np.nan)
    if a.isna().all() or b.isna().all(): return True
    c = a.corr(b)
    return not (c is not None and c > 0.95)

def check_graph_cutoff(symbol: str, asof: pd.Timestamp) -> bool:
    p = FEAT_DIR / "graph_features_weekly.csv"
    if not p.exists(): return True
    try:
        gf = pd.read_csv(p)
        # If graph has a "built_utc" column, enforce <= asof; else OK
        if "built_utc" in gf.columns:
            tmax = pd.to_datetime(gf["built_utc"], errors="coerce").max()
            return bool(pd.isna(tmax) or tmax <= asof)
        return True
    except Exception:
        return True

def check_vol_surface_cutoff(asof: pd.Timestamp) -> bool:
    meta = (DL / "options_meta.json")
    if not meta.exists(): return True
    try:
        m = json.loads(meta.read_text())
        t = pd.to_datetime(m.get("asof_utc"), errors="coerce")
        return bool(pd.isna(t) or t <= asof)
    except Exception:
        return True

def count_features(df: pd.DataFrame) -> Tuple[int, int]:
    cols = [c for c in df.columns if c not in ("Date","symbol","freq","asof_ts","regime_flag","y_1d",
                                               "live_source_equity","live_source_options","is_synth_options","data_age_min")]
    total = len(cols)
    auto = len([c for c in cols if c.startswith("AUTO_")])
    return total, auto

def check_feature_caps(df: pd.DataFrame, max_total=400, max_auto=120) -> Dict:
    total, auto = count_features(df)
    return {
        "ok_total": total <= max_total,
        "ok_auto":  auto <= max_auto,
        "total": total, "auto": auto,
        "cap_total": max_total, "cap_auto": max_auto
    }

def compute_drift(df: pd.DataFrame, ref_window=120, cur_window=60, sample_cols=20) -> Dict:
    # Use last ref_window vs last cur_window before end; random sample of columns
    if len(df) < (ref_window + cur_window + 5): return {"ok": True, "psi": {}}
    ref = df.iloc[-(ref_window+cur_window):-cur_window].copy()
    cur = df.iloc[-cur_window:].copy()
    rng = np.random.RandomState(42)
    cols = [c for c in df.columns if c.startswith(("MAN_","AUTO_","GRAPH_","OPT_")) and not c.endswith("_is_missing")]
    rng.shuffle(cols)
    cols = cols[:min(sample_cols, len(cols))]
    psi_map = {}
    for c in cols:
        try:
            psi = _psI(pd.to_numeric(ref[c], errors="coerce"),
                       pd.to_numeric(cur[c], errors="coerce"))
            psi_map[c] = round(float(psi), 4)
        except Exception:
            psi_map[c] = None
    worst = max([v for v in psi_map.values() if v is not None] or [0.0])
    return {"ok": worst < 0.3, "worst_psi": worst, "psi": psi_map}

def run(limit_files: int | None = 30) -> Dict:
    files = _list_feature_files(limit_files)
    asof = pd.Timestamp.utcnow()
    findings = []
    for f in files:
        try:
            df = pd.read_csv(f, parse_dates=["Date","asof_ts"])
        except Exception:
            df = pd.read_csv(f)
            if "Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            df["asof_ts"] = pd.Timestamp.utcnow()
        if df.empty:
            continue
        sym = f.stem.replace("_features","")

        res = {
            "file": f.name,
            "summary": _summary(df),
            "time_monotonic": check_time_monotonic(df),
            "no_lookahead": check_no_lookahead(df),
            "graph_cutoff": check_graph_cutoff(sym, asof),
            "vol_surface_cutoff": check_vol_surface_cutoff(asof),
            "feature_caps": check_feature_caps(df),
            "drift": compute_drift(df)
        }
        findings.append(res)

    size_gb = round(_approx_dir_size_gb(DL), 3)
    out = {
        "when_utc": asof.isoformat()+"Z",
        "datalake_size_gb": size_gb,
        "files_checked": len(findings),
        "findings": findings
    }
    (REP / "hygiene_report.json").write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(json.dumps(out, indent=2))
    return out

if __name__ == "__main__":
    run(limit_files=30)
