# src/symbols.py
from __future__ import annotations
import pandas as pd
from pathlib import Path

DL = Path("datalake"); SYM = DL / "symbols"; SYM.mkdir(parents=True, exist_ok=True)

def normalize_symbol(s: str) -> str:
    return str(s).strip().upper().replace("&","AND").replace("-","")

def update_index_membership(latest_list_csv: str) -> int:
    """
    Provide a CSV path with columns: Symbol, Sector (latest constituents).
    This function writes datalake/symbols/nifty500_members.csv and
    appends a dated snapshot to datalake/symbols/membership_history.csv
    """
    d = pd.read_csv(latest_list_csv)
    d["Symbol"] = d["Symbol"].map(normalize_symbol)
    d["Sector"] = d.get("Sector","").astype(str).str.upper()
    d.to_csv(SYM / "nifty500_members.csv", index=False)

    hist = SYM / "membership_history.csv"
    d2 = d.copy(); d2["asof"] = pd.Timestamp.utcnow().normalize().date().isoformat()
    if hist.exists():
        old = pd.read_csv(hist)
        d2 = pd.concat([old, d2], ignore_index=True)
    d2.to_csv(hist, index=False)
    return len(d)
