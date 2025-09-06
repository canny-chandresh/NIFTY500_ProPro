# src/exp_tracker.py
from __future__ import annotations
import os, json, datetime as dt
from pathlib import Path

EXP_DIR = Path("reports/experiments")
EXP_DIR.mkdir(parents=True, exist_ok=True)

def log_experiment(name: str, params: dict, metrics: dict, tags: dict | None = None) -> str:
    run_id = f"{name}_{dt.datetime.utcnow().strftime('%Y%m%d_%H%M%SZ')}"
    payload = {
        "run_id": run_id,
        "name": name,
        "when_utc": dt.datetime.utcnow().isoformat()+"Z",
        "params": params or {},
        "metrics": metrics or {},
        "tags": tags or {}
    }
    path = EXP_DIR / f"{run_id}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    # append index
    idx = EXP_DIR / "index.json"
    try:
        j = json.load(open(idx)) if idx.exists() else []
    except Exception:
        j = []
    j.append({"run_id": run_id, "name": name, "metrics": metrics})
    json.dump(j[-1000:], open(idx,"w"), indent=2)
    return str(path)
