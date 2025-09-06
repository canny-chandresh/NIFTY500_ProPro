# src/config_guard.py
from __future__ import annotations
import os, json, hashlib
from pathlib import Path

SNAP = Path("reports/metrics/config_last.json")

def _hash(v) -> str:
    try:
        b = json.dumps(v, sort_keys=True).encode()
        return hashlib.sha1(b).hexdigest()
    except Exception:
        return "NA"

def config_diff(current: dict) -> dict:
    prev = {}
    if SNAP.exists():
        try: prev = json.load(SNAP.open())
        except Exception: pass
    prev_hash = _hash(prev)
    cur_hash = _hash(current)

    # compute shallow diff
    changed = {}
    keys = set(prev.keys()) | set(current.keys())
    for k in sorted(keys):
        pv, cv = prev.get(k), current.get(k)
        if pv != cv:
            changed[k] = {"old": pv, "new": cv}

    SNAP.parent.mkdir(parents=True, exist_ok=True)
    json.dump(current, SNAP.open("w"), indent=2)
    return {"changed": changed, "prev_hash": prev_hash, "cur_hash": cur_hash}
