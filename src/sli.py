# src/sli.py
from __future__ import annotations
import os, json, time, datetime as dt
from pathlib import Path
import pandas as pd

REP = Path("reports"); MET = REP / "metrics"; LOG = REP / "logs"
for p in (REP, MET, LOG): p.mkdir(parents=True, exist_ok=True)

def compute_sli(datalake_dir="datalake", expected_symbols=500, symbol_dir="per_symbol") -> dict:
    t0 = time.time()
    DL = Path(datalake_dir)
    ok = DL.exists()
    per = DL / symbol_dir
    n_files = len(list(per.glob("*.csv"))) if per.exists() else 0
    # completeness vs expected
    comp = n_files / max(1, expected_symbols)

    # freshness heuristic: daily_equity file exists and touched today
    daily = DL / "daily_equity.parquet"
    fresh = False
    if daily.exists():
        mtime = dt.datetime.utcfromtimestamp(daily.stat().st_mtime)
        fresh = (dt.datetime.utcnow().date() - mtime.date()).days <= 1

    payload = {
        "when_utc": dt.datetime.utcnow().isoformat()+"Z",
        "datalake_exists": ok, "per_symbol_files": n_files,
        "completeness": comp, "fresh": fresh,
        "latency_sec": round(time.time()-t0, 3)
    }
    (MET / "sli_latest.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return payload

def alert_if_bad(sli: dict, tg_send=None, min_comp=0.70, require_fresh=True):
    bad = (sli["completeness"] < min_comp) or (require_fresh and not sli["fresh"])
    if bad and tg_send:
        msg = (f"⚠️ Data SLI breach\n"
               f"- completeness: {sli['completeness']:.2%}\n"
               f"- fresh: {sli['fresh']}\n"
               f"- per_symbol_files: {sli['per_symbol_files']}")
        try: tg_send(header=msg)
        except Exception: pass
    return not bad
