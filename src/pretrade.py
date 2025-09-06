# src/pretrade.py
from __future__ import annotations
import os, json, hashlib, datetime as dt
from pathlib import Path
from typing import List, Dict
import pandas as pd
try:
    import yaml
except Exception:
    yaml = None

AUD = Path("reports/audit"); AUD.mkdir(parents=True, exist_ok=True)
RULES = Path("config/pretrade_rules.yaml")

def _hash_line(s: str) -> str: return hashlib.sha256(s.encode("utf-8")).hexdigest()

def load_rules() -> dict:
    if yaml is None or not RULES.exists(): 
        return {"max_exposure":1.0,"max_per_name":0.25,"do_not_trade":[],"block_times":[]}
    return yaml.safe_load(RULES.read_text())

def check_orders(orders: pd.DataFrame, rules: dict) -> List[str]:
    if orders is None or orders.empty: return []
    msgs = []
    tot = orders.get("size_pct", pd.Series([0])).sum()
    if tot > float(rules.get("max_exposure", 1.0)) + 1e-6:
        msgs.append(f"exposure>{rules.get('max_exposure')}")
    per = orders.groupby("Symbol")["size_pct"].sum().max()
    if per > float(rules.get("max_per_name", 0.25)) + 1e-6:
        msgs.append(f"per_name>{rules.get('max_per_name')}")
    dnt = set(map(str.upper, rules.get("do_not_trade", [])))
    bad = [s for s in orders["Symbol"].astype(str).str.upper() if s in dnt]
    if bad: msgs.append(f"DNT:{','.join(sorted(set(bad)))}")
    # time windows are enforced in workflow; here we just note the rule presence
    return msgs

def append_audit(run_id: str, context: dict, orders_auto: pd.DataFrame, orders_algo: pd.DataFrame, violations: List[str]):
    ts = dt.datetime.utcnow().isoformat()+"Z"
    rec = {"when_utc": ts, "run_id": run_id, "context": context,
           "auto_rows": int(0 if orders_auto is None else len(orders_auto)),
           "algo_rows": int(0 if orders_algo is None else len(orders_algo)),
           "violations": violations}
    line = json.dumps(rec, separators=(",",":"))
    h = _hash_line(line)
    with open(AUD / "pretrade_audit.log","a", encoding="utf-8") as f:
        f.write(line + f"\t{h}\n")
