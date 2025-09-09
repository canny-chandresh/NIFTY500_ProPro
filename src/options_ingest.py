# -*- coding: utf-8 -*-
"""
options_ingest.py — NSE options chain primary, synthetic fallback for training.
Writes JSON lines to datalake/options/chain_<SYMBOL>.jsonl (latest snapshot).
"""
from __future__ import annotations
import json, math, time, traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List

import pandas as pd
import numpy as np

from config import CONFIG

try:
    from data_sources.nse_client import options_chain as nse_chain
except Exception:
    nse_chain = None

DL = Path(CONFIG["paths"]["datalake"])
OPT = DL / "options"
RPTDBG = Path(CONFIG["paths"]["reports"]) / "debug"
OPT.mkdir(parents=True, exist_ok=True); RPTDBG.mkdir(parents=True, exist_ok=True)

def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()

def _save_jsonl(path: Path, obj: Dict[str,Any]):
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj) + "\n")

def _synthetic_chain(underlying: float, iv_approx: float=0.18, step: int=50, wings: int=10) -> Dict[str,Any]:
    # generate simple strikes around ATM
    atm = int(round(underlying / step) * step)
    strikes = [atm + step*i for i in range(-wings, wings+1)]
    out = {"underlying": underlying, "iv_proxy": iv_approx, "strikes": [], "ts": _utcnow(), "synthetic": True}
    for k in strikes:
        # toy greeks & prices (do not use to trade live!)
        dist = abs(k - underlying)/max(underlying,1e-6)
        price = max(0.1, underlying*0.01*(1.0 - min(0.9, dist*4)))
        out["strikes"].append({"strike": k, "CE": {"lastPrice": price}, "PE": {"lastPrice": price*0.9}})
    return out

def fetch_and_store() -> Dict[str,Any]:
    cfg = CONFIG.get("options", {})
    if not cfg.get("enabled", True):
        return {"ok": False, "reason": "options disabled"}
    stats = {"ok": True, "written": 0, "sources": {"nse":0,"synthetic":0}}
    # Indices
    for idx in cfg.get("indices", []):
        try:
            js = nse_chain(idx, is_index=True) if nse_chain else None
            if not js or "records" not in js:
                # synth fallback uses last close if available
                u = 20000.0
                syn = _synthetic_chain(u)
                _save_jsonl(OPT / f"chain_{idx}.jsonl", syn)
                stats["sources"]["synthetic"] += 1
                continue
            _save_jsonl(OPT / f"chain_{idx}.jsonl", {"ts": _utcnow(), "symbol": idx, "records": js.get("records",{}), "synthetic": False})
            stats["sources"]["nse"] += 1
            stats["written"] += 1
            time.sleep(0.8)
        except Exception:
            traceback.print_exc()

    # Stocks
    for s in cfg.get("stocks", []):
        try:
            js = nse_chain(s, is_index=False) if nse_chain else None
            if not js or "records" not in js:
                syn = _synthetic_chain(underlying=2500.0)
                _save_jsonl(OPT / f"chain_{s}.jsonl", syn)
                stats["sources"]["synthetic"] += 1
                continue
            _save_jsonl(OPT / f"chain_{s}.jsonl", {"ts": _utcnow(), "symbol": s, "records": js.get("records",{}), "synthetic": False})
            stats["sources"]["nse"] += 1
            stats["written"] += 1
            time.sleep(0.8)
        except Exception:
            traceback.print_exc()
    (RPTDBG / "options_ingest_summary.txt").write_text(str(stats))
    print(stats)
    return stats

if __name__ == "__main__":
    fetch_and_store()
