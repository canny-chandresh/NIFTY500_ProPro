# src/model_registry.py
from __future__ import annotations
import os, json, hashlib, datetime as dt
from pathlib import Path
from config import CONFIG

def _hash_dict(d: dict) -> str:
    raw = json.dumps(d, sort_keys=True).encode("utf-8")
    return hashlib.sha1(raw).hexdigest()

def register_model(meta: dict) -> str:
    """
    meta should include: {"name": "...", "params": {...}, "metrics": {...}}
    Stores under reports/registry and keeps last N entries.
    Returns model_id (sha1).
    """
    reg = CONFIG.get("registry", {})
    if not reg.get("enabled", True): return ""
    root = Path(reg.get("dir","reports/registry")); root.mkdir(parents=True, exist_ok=True)
    mid = _hash_dict({"name": meta.get("name"), "params": meta.get("params", {})})
    payload = {
        "model_id": mid, "when_utc": dt.datetime.utcnow().isoformat()+"Z",
        "meta": meta
    }
    (root / f"{mid}.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")

    # index
    idx = root / "index.json"
    items = []
    if idx.exists():
        try: items = json.loads(idx.read_text())
        except Exception: items = []
    items = [x for x in items if x.get("model_id") != mid]
    items.append({"model_id": mid, "when_utc": payload["when_utc"], "name": meta.get("name")})
    items = items[-int(reg.get("keep_last", 20)):]
    idx.write_text(json.dumps(items, indent=2), encoding="utf-8")
    return mid
