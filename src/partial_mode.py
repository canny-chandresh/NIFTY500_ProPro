# src/partial_mode.py
from __future__ import annotations
import os, json, datetime as dt
from pathlib import Path
from typing import Dict, List

STATE = Path("reports/metrics/partial_state.json")
STATE.parent.mkdir(parents=True, exist_ok=True)

DEFAULT = {
    "enabled": True,            # allow partial execution
    "min_features_ok": 0.60,    # proceed if >=60% planned features present
    "min_symbols_ok":  0.50,    # proceed if per_symbol csv coverage >=50% of universe
}

def _write(obj: Dict):
    obj = {**obj, "updated_utc": dt.datetime.utcnow().isoformat()+"Z"}
    STATE.write_text(json.dumps(obj, indent=2), encoding="utf-8")

def check_inputs(planned_features: List[str], found_features: List[str], planned_symbols: int, found_symbols: int, cfg: Dict=None) -> Dict:
    cfg = {**DEFAULT, **(cfg or {})}
    feat_cov = 0.0 if not planned_features else len(set(found_features) & set(planned_features)) / max(1, len(planned_features))
    sym_cov  = (found_symbols / max(1, planned_symbols))
    ok = (feat_cov >= cfg["min_features_ok"]) and (sym_cov >= cfg["min_symbols_ok"])
    out = {"partial_allowed": cfg["enabled"], "ok": ok, "feature_coverage": feat_cov, "symbol_coverage": sym_cov}
    _write(out)
    return out

def is_partial_active() -> bool:
    if not STATE.exists(): return False
    try:
        st = json.loads(STATE.read_text())
        return (not st.get("ok", False)) and bool(st.get("partial_allowed", True))
    except Exception:
        return False
