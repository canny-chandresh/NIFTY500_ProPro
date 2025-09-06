# src/calendars.py
from __future__ import annotations
import pandas as pd
from pathlib import Path
import datetime as dt
from typing import Tuple

DL = Path("datalake"); CAL = DL / "calendars"; CAL.mkdir(parents=True, exist_ok=True)

EARN = CAL / "earnings.csv"     # Symbol, date (YYYY-MM-DD)
EXDIV= CAL / "ex_div.csv"       # Symbol, date
MACRO= CAL / "macro.csv"        # event, date, importance (HIGH/MED/LOW)

def _read(path: Path) -> pd.DataFrame:
    if not path.exists(): return pd.DataFrame()
    try: 
        d = pd.read_csv(path)
        for c in ("date","when","asof"):
            if c in d.columns: d[c] = pd.to_datetime(d[c], errors="coerce")
        return d
    except Exception:
        return pd.DataFrame()

def policy_window_block(symbol: str, today: pd.Timestamp, pre_days=1, post_days=0) -> bool:
    """
    Returns True if symbol should be BLOCKED for trading due to near-term events.
    """
    sym = str(symbol).upper()
    d0 = pd.to_datetime(today).normalize()
    # earnings block
    e = _read(EARN)
    if not e.empty:
        e = e[e["Symbol"].astype(str).str.upper() == sym]
        if not e.empty:
            if any(abs((d0 - pd.to_datetime(x)).days) <= pre_days for x in e["date"]): 
                return True
    # ex-dividend block
    ex = _read(EXDIV)
    if not ex.empty:
        ex = ex[ex["Symbol"].astype(str).str.upper() == sym]
        if not ex.empty:
            if any(abs((d0 - pd.to_datetime(x)).days) <= pre_days for x in ex["date"]):
                return True
    return False

def macro_block(today: pd.Timestamp, pre_hours=1) -> bool:
    """
    Returns True if market-wide macro events today warrant blocking new entries.
    """
    m = _read(MACRO)
    if m.empty: return False
    m["date"] = pd.to_datetime(m["date"], errors="coerce")
    m = m[m["importance"].astype(str).str.upper() == "HIGH"]
    return any(pd.to_datetime(x).date() == pd.to_datetime(today).date() for x in m["date"])
