# src/feature_audit.py
from __future__ import annotations
import os, json, glob, importlib
from pathlib import Path

CHECKS = {
    "config_flags": [
        ("features.regime_v1", True),
        ("features.options_sanity", True),
        ("features.sr_pivots_v1", True),
        ("features.reports_v1", True),
        ("features.killswitch_v1", True),
        ("features.drift_alerts", True),
        ("features.walkforward_v1", True),
    ],
    "modules": [
        "pipeline_ai","model_selector","ai_policy","risk_manager","atr_tuner",
        "telegram","entrypoints","error_logger","archiver",
        "market_hours","validator","locks","config_guard","metrics_tracker",
        "report_eod","report_periodic","regime","events","sector","smartmoney"
    ],
    "files": [
        "datalake/holidays_nse.csv",
        "reports",
        "datalake",
    ]
}

def _get(cfg: dict, path: str):
    cur = cfg
    for p in path.split("."):
        if not isinstance(cur, dict) or p not in cur:
            return None
        cur = cur[p]
    return cur

def audit() -> dict:
    out = {"flags": {}, "modules": {}, "files": {}}
    try:
        import config
        cfg = config.CONFIG
    except Exception as e:
        return {"error": f"config import error: {e}"}
    # flags
    for key, expect in CHECKS["config_flags"]:
        v = _get(cfg, key)
        out["flags"][key] = {"value": v, "ok": (v == expect)}
    # modules
    for m in CHECKS["modules"]:
        try:
            importlib.import_module(m)
            out["modules"][m] = {"ok": True}
        except Exception as e:
            out["modules"][m] = {"ok": False, "err": repr(e)}
    # files
    for f in CHECKS["files"]:
        out["files"][f] = {"ok": os.path.exists(f)}
    return out

if __name__ == "__main__":
    print(json.dumps(audit(), indent=2))
