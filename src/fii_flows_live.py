# src/fii_flows_live.py
"""
Attempts to fetch daily FII/DII net flows (INR crores) from polite endpoints.
If it fails, falls back to datalake/dii_fii_flows.csv (seed).
Writes: datalake/flows/fii_dii_<YYYYMMDD>.csv and flows_latest.csv
"""

from __future__ import annotations
from pathlib import Path
import csv, datetime as dt
from typing import Dict, Any, List

import pandas as pd

try:
    import requests
except Exception:
    requests = None

DL = Path("datalake")
FLOW_DIR = DL / "flows"
FLOW_DIR.mkdir(parents=True, exist_ok=True)

def _seed_fallback() -> pd.DataFrame:
    seed = DL / "fii_dii_flows.csv"
    if seed.exists():
        try:
            return pd.read_csv(seed, parse_dates=["date"])
        except Exception:
            pass
    return pd.DataFrame(columns=["date","fii_net","dii_net","source"])

def fetch_flows() -> pd.DataFrame:
    # Placeholder polite request; if blocked, use seed.
    if requests is not None:
        try:
            # Example placeholder (replace with a public CSV endpoint if you have one)
            # r = requests.get("https://example.com/fii_dii_daily.csv", timeout=10)
            # df = pd.read_csv(io.StringIO(r.text))
            raise RuntimeError("no_public_endpoint_configured")
        except Exception:
            pass
    df = _seed_fallback()
    if "source" not in df.columns:
        df["source"] = "seed"
    return df

def write_latest(df: pd.DataFrame) -> Dict:
    if df.empty:
        return {"ok": False, "reason": "empty"}
    df = df.sort_values("date")
    today = dt.datetime.utcnow().strftime("%Y%m%d")
    p = FLOW_DIR / f"fii_dii_{today}.csv"
    df.to_csv(p, index=False)
    (FLOW_DIR / "flows_latest.csv").write_text(p.read_text(encoding="utf-8"), encoding="utf-8")
    return {"ok": True, "path": str(p), "rows": int(len(df))}

if __name__ == "__main__":
    print(write_latest(fetch_flows()))
