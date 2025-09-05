from __future__ import annotations
import datetime as dt
from typing import Tuple
import pandas as pd

try:
    from config import CONFIG
except Exception:
    CONFIG = {}

def _now_ist() -> dt.datetime:
    try:
        from utils_time import now_ist
        return now_ist()
    except Exception:
        return dt.datetime.utcnow() + dt.timedelta(hours=5, minutes=30)

def _next_month_end_weekday_ist() -> dt.date:
    n = _now_ist().date()
    year, month = n.year, n.month
    import calendar
    last_dom = calendar.monthrange(year, month)[1]
    d = dt.date(year, month, last_dom)
    while d.weekday() > 4:
        d -= dt.timedelta(days=1)
    if d < n:
        nm_year, nm_month = (year + (month == 12), (month % 12) + 1)
        last_dom = calendar.monthrange(nm_year, nm_month)[1]
        d = dt.date(nm_year, nm_month, last_dom)
        while d.weekday() > 4:
            d -= dt.timedelta(days=1)
    return d

def _apply_fut_sl(entry: float, sl: float) -> float:
    mx = float(CONFIG.get("futures", {}).get("max_sl_pct", 0.25))
    floor = entry * (1.0 - mx)
    return max(sl, floor)

def simulate_from_equity_recos(
    equity_rows: pd.DataFrame,
    max_rows: int = 3
) -> Tuple[pd.DataFrame, str]:
    """
    Map equity selections to simple futures paper orders (synthetic).
    Returns (df, source_tag) with source_tag='synthetic'.
    Columns:
      Timestamp, Symbol, UnderlyingType, Exchange, Expiry,
      EntryPrice, SL, Target, Lots, Reason
    """
    src_tag = "synthetic"
    cols_needed = {"Symbol", "Entry", "SL", "Target"}
    if equity_rows is None or equity_rows.empty or not cols_needed.issubset(equity_rows.columns):
        return pd.DataFrame(columns=[
            "Timestamp","Symbol","UnderlyingType","Exchange","Expiry",
            "EntryPrice","SL","Target","Lots","Reason"
        ]), src_tag

    lots_default = int(CONFIG.get("futures", {}).get("lots_default", 1))
    rows = []
    now_iso = _now_ist().isoformat()
    exp = _next_month_end_weekday_ist()

    for r in equity_rows.head(max_rows).itertuples():
        sym = str(getattr(r, "Symbol"))
        entry = float(getattr(r, "Entry", 0) or 0.0)
        sl    = float(getattr(r, "SL", 0) or 0.0)
        tgt   = float(getattr(r, "Target", 0) or 0.0)
        if entry <= 0:
            continue
        sl = _apply_fut_sl(entry, sl)

        rows.append({
            "Timestamp": now_iso,
            "Symbol": sym,
            "UnderlyingType": "INDEX" if any(k in sym.upper() for k in ("NIFTY","BANKNIFTY","FINNIFTY","MIDCPNIFTY")) else "EQUITY",
            "Exchange": "NSE",
            "Expiry": str(exp),
            "EntryPrice": round(entry, 2),
            "SL": round(sl, 2),
            "Target": round(tgt, 2),
            "Lots": lots_default,
            "Reason": "synthetic FUT mirror of equity levels"
        })

    return pd.DataFrame(rows), src_tag
