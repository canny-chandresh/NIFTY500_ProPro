# src/feature_spec.py
"""
Feature Spec Loader & Validator
- Loads config/feature_spec.yaml (defensive if missing)
- Validates feature matrices in datalake/features/*.csv
- Checks:
    * required keys present
    * must_have and should_have features
    * only whitelisted namespaces (MAN_, AUTO_, GRAPH_, OPT_)
    * caps on total features and AUTO_* count
- Emits report:
    reports/hygiene/feature_spec_report.json
    reports/hygiene/feature_spec_report.txt
"""

from __future__ import annotations
from pathlib import Path
from typing import Dict, List, Tuple
import json
import pandas as pd
import numpy as np

try:
    import yaml
except Exception:
    yaml = None

ROOT = Path(".")
CONF = ROOT / "config" / "feature_spec.yaml"
DL = ROOT / "datalake"
FEAT = DL / "features"
OUTDIR = ROOT / "reports" / "hygiene"
OUTDIR.mkdir(parents=True, exist_ok=True)

DEFAULT_SPEC = {
    "meta": {"required_keys": ["symbol","freq","asof_ts","regime_flag","y_1d"]},
    "namespaces": {"allow_prefixes": ["MAN_","AUTO_","GRAPH_","OPT_"]},
    "caps": {"max_total_features": 400, "max_auto_features": 120},
    "core": {"must_have": ["MAN_ret1","MAN_atr14","MAN_ema20slope"]},
    "graph": {"should_have": ["GRAPH_deg","GRAPH_btw"]},
    "options": {"optional": ["OPT_iv","OPT_oi","OPT_pcr"]},
    "drift": {"psi_warn": 0.2, "psi_fail": 0.3}
}

def load_spec() -> Dict:
    if not CONF.exists() or yaml is None:
        return DEFAULT_SPEC
    try:
        spec = yaml.safe_load(CONF.read_text()) or {}
        # merge shallow with defaults to avoid missing keys
        def _merge(a,b):
            out = dict(a); out.update(b); return out
        S = dict(DEFAULT_SPEC)
        for k,v in spec.items():
            if isinstance(v, dict) and isinstance(S.get(k), dict):
                S[k] = _merge(S[k], v)
            else:
                S[k] = v
        return S
    except Exception:
        return DEFAULT_SPEC

def _columns_by_kind(df: pd.DataFrame) -> Dict[str, List[str]]:
    base_exclude = {"Date","symbol","freq","asof_ts","regime_flag","y_1d",
                    "live_source_equity","live_source_options","is_synth_options","data_age_min"}
    feats = [c for c in df.columns if c not in base_exclude and not c.endswith("_is_missing")]
    out = {
        "MAN": [c for c in feats if c.startswith("MAN_")],
        "AUTO": [c for c in feats if c.startswith("AUTO_")],
        "GRAPH": [c for c in feats if c.startswith("GRAPH_")],
        "OPT": [c for c in feats if c.startswith("OPT_")],
        "OTHER": [c for c in feats if not any(c.startswith(p) for p in ("MAN_","AUTO_","GRAPH_","OPT_"))]
    }
    return out

def _psi(a: pd.Series, b: pd.Series, bins: int = 10) -> float:
    a, b = a.dropna(), b.dropna()
    if len(a) < 100 or len(b) < 100: return 0.0
    qs = np.unique(np.quantile(a, np.linspace(0,1,bins+1)))
    ah, _ = np.histogram(a, bins=qs)
    bh, _ = np.histogram(b, bins=qs)
    ap = np.where(ah==0, 1e-6, ah) / max(1, ah.sum())
    bp = np.where(bh==0, 1e-6, bh) / max(1, bh.sum())
    return float(np.sum((ap - bp) * np.log(ap / bp)))

def validate_matrix(df: pd.DataFrame, spec: Dict) -> Dict:
    result = {"ok": True, "errors": [], "warnings": []}
    # 1) required keys
    req = spec.get("meta",{}).get("required_keys", [])
    missing = [k for k in req if k not in df.columns]
    if missing:
        result["ok"] = False
        result["errors"].append(f"missing required keys: {missing}")

    # 2) must_have / should_have / optional
    must = spec.get("core",{}).get("must_have", []) + spec.get("graph",{}).get("must_have", [])
    should = spec.get("graph",{}).get("should_have", [])
    opt = spec.get("options",{}).get("optional", [])
    miss_must = [c for c in must if c and c not in df.columns]
    if miss_must:
        result["ok"] = False
        result["errors"].append(f"missing must_have features: {miss_must}")
    miss_should = [c for c in should if c and c not in df.columns]
    if miss_should:
        result["warnings"].append(f"missing should_have features: {miss_should}")

    # 3) namespace allowlist
    allow = tuple(spec.get("namespaces",{}).get("allow_prefixes", []))
    kinds = _columns_by_kind(df)
    if kinds["OTHER"]:
        result["warnings"].append(f"unexpected feature namespaces: {kinds['OTHER'][:10]} (+{max(0,len(kinds['OTHER'])-10)} more)")
        # Do not fail; warn only (you may be experimenting)

    # 4) caps
    max_total = int(spec.get("caps",{}).get("max_total_features", 400))
    max_auto  = int(spec.get("caps",{}).get("max_auto_features", 120))
    total = sum(len(v) for v in kinds.values())
    if total > max_total:
        result["ok"] = False
        result["errors"].append(f"feature count {total} exceeds cap {max_total}")
    if len(kinds["AUTO"]) > max_auto:
        result["ok"] = False
        result["errors"].append(f"AUTO_* count {len(kinds['AUTO'])} exceeds cap {max_auto}")

    # 5) drift quick check (optional informational)
    drift = {}
    if "Date" in df.columns:
        df2 = df.copy()
        try:
            df2["Date"] = pd.to_datetime(df2["Date"], errors="coerce")
            df2 = df2.sort_values("Date")
            if len(df2) >= 200:
                ref = df2.iloc[-200:-100]
                cur = df2.iloc[-100:]
                sample_cols = [c for c in kinds["MAN"][:5] + kinds["AUTO"][:5] + kinds["GRAPH"][:5] if c in df2.columns]
                for c in sample_cols:
                    drift[c] = round(_psi(pd.to_numeric(ref[c], errors="coerce"),
                                          pd.to_numeric(cur[c], errors="coerce")), 4)
        except Exception:
            pass
    result["drift_sample_psi"] = drift
    return result

def validate_repo(limit_files: int | None = 50) -> Dict:
    spec = load_spec()
    files = sorted(FEAT.glob("*_features.csv"))
    if limit_files: files = files[:limit_files]
    summaries = []
    for p in files:
        try:
            df = pd.read_csv(p)
        except Exception:
            continue
        res = validate_matrix(df, spec)
        summaries.append({
            "file": p.name,
            "ok": res["ok"],
            "errors": res["errors"],
            "warnings": res["warnings"],
            "drift_sample_psi": res.get("drift_sample_psi", {})
        })
    overall_ok = all(s["ok"] for s in summaries) if summaries else True
    out = {
        "overall_ok": overall_ok,
        "checked_files": len(summaries),
        "spec": spec,
        "summaries": summaries
    }
    # write reports
    (OUTDIR / "feature_spec_report.json").write_text(json.dumps(out, indent=2), encoding="utf-8")
    # lightweight TXT
    lines = [f"FEATURE SPEC REPORT (files={len(summaries)})",
             "OK" if overall_ok else "SOME FAILURES", "-"*60]
    for s in summaries[:100]:
        flag = "OK" if s["ok"] else "FAIL"
        lines.append(f"{flag} :: {s['file']}")
        if s["errors"]:
            for e in s["errors"][:5]:
                lines.append(f"  ERR - {e}")
        if s["warnings"]:
            for w in s["warnings"][:5]:
                lines.append(f"  WRN - {w}")
    (OUTDIR / "feature_spec_report.txt").write_text("\n".join(lines), encoding="utf-8")
    print("\n".join(lines))
    return out

if __name__ == "__main__":
    validate_repo()
