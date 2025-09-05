from __future__ import annotations
import math, datetime as dt
from typing import Tuple, Optional
import pandas as pd

try:
    from config import CONFIG
except Exception:
    CONFIG = {}

# ---------- Time helpers (IST-aware if utils_time is present) ----------
def _now_ist() -> dt.datetime:
    try:
        from utils_time import now_ist
        return now_ist()
    except Exception:
        return dt.datetime.utcnow() + dt.timedelta(hours=5, minutes=30)

def _next_thursday_ist() -> dt.date:
    """Next Thursday (weekly-style) from 'now' in IST."""
    n = _now_ist().date()
    # weekday(): Mon=0 ... Sun=6; Thursday=3
    days_ahead = (3 - n.weekday()) % 7
    days_ahead = 7 if days_ahead == 0 else days_ahead  # always next
    return n + dt.timedelta(days=days_ahead)

def _next_month_end_weekday_ist() -> dt.date:
    """Monthly-ish expiry: last weekday (Fri or earlier) of current/next month."""
    n = _now_ist().date()
    # Pick this month if still far from month end; else next month
    year, month = n.year, n.month
    # final day of month
    if month == 12:
        lm_year, lm_month = year, 12
    else:
        lm_year, lm_month = year, month
    import calendar
    last_dom = calendar.monthrange(lm_year, lm_month)[1]
    d = dt.date(lm_year, lm_month, last_dom)
    while d.weekday() > 4:
        d -= dt.timedelta(days=1)
    if d < n:  # already passed; take next month
        nm_year, nm_month = (year + (month == 12), (month % 12) + 1)
        last_dom = calendar.monthrange(nm_year, nm_month)[1]
        d = dt.date(nm_year, nm_month, last_dom)
        while d.weekday() > 4:
            d -= dt.timedelta(days=1)
    return d

# ---------- Heuristics ----------
_INDEX_HINTS = ("NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY")

def _is_index(sym: str) -> bool:
    s = (sym or "").upper()
    return any(k in s for k in _INDEX_HINTS)

def _strike_step(underlying: float, is_index: bool) -> int:
    if is_index:
        # Rough heuristic
        if underlying >= 30000:  # BANKNIFTY style
            return 100
        return 50               # NIFTY style
    # Stocks — rough bands
    if underlying >= 1000: return 10
    if underlying >= 500:  return 5
    if underlying >= 200:  return 2
    return 1

def _round_to_step(x: float, step: int) -> float:
    return round(step * round(float(x) / step), 2)

def _choose_expiry(sym: str) -> dt.date:
    return _next_thursday_ist() if _is_index(sym) else _next_month_end_weekday_ist()

def _synthetic_option_price(underlying: float, atm: bool = True) -> float:
    """
    Synthetic placeholder, no live NSE data:
    ATM premium ~ 2% of spot; OTM would be lower. Adjust as needed.
    """
    base = max(1.0, 0.02 * float(underlying))
    return round(base, 2)

def _apply_sanity_sl(entry_price: float, sl_price: float) -> float:
    mx = float(CONFIG.get("options", {}).get("max_sl_pct", 0.25))
    # ensure SL no more than mx below entry for long options
    floor = entry_price * (1.0 - mx)
    return max(sl_price, floor)

# ---------- Public API ----------
def simulate_from_equity_recos(
    equity_rows: pd.DataFrame,
    max_legs: int = 3
) -> Tuple[pd.DataFrame, str]:
    """
    Map equity picks to simple single-leg options (synthetic).
    Returns (df, source_tag) with source_tag='synthetic'.
    Columns produced:
      Timestamp, Symbol, UnderlyingType, UnderlyingPrice, Exchange, Expiry,
      Strike, Leg, Qty, EntryPrice, SL, Target, RR, OI, IV, Reason
    """
    src_tag = "synthetic"
    cols_needed = {"Symbol", "Entry", "SL", "Target"}
    if equity_rows is None or equity_rows.empty or not cols_needed.issubset(equity_rows.columns):
        return pd.DataFrame(columns=[
            "Timestamp","Symbol","UnderlyingType","UnderlyingPrice","Exchange","Expiry",
            "Strike","Leg","Qty","EntryPrice","SL","Target","RR","OI","IV","Reason"
        ]), src_tag

    out = []
    now_iso = _now_ist().isoformat()
    for r in equity_rows.head(max_legs).itertuples():
        sym = str(getattr(r, "Symbol"))
        entry = float(getattr(r, "Entry", 0) or 0.0)
        sl    = float(getattr(r, "SL", 0) or 0.0)
        tgt   = float(getattr(r, "Target", 0) or 0.0)
        if entry <= 0: 
            continue

        idx = _is_index(sym)
        step = _strike_step(entry, idx)
        strike = _round_to_step(entry, step)
        expiry = _choose_expiry(sym)

        # Pick CE when probability implies bullish bias (use target>entry),
        # else PE. (You can replace with your model's signed signal.)
        leg = "CE" if tgt >= entry else "PE"

        # Synthetic premiums: ATM approx
        opt_entry = _synthetic_option_price(entry, atm=True)
        # Risk/target translated crudely via % move
        up_pct  = (tgt - entry) / entry
        dn_pct  = max(1e-6, (entry - sl) / entry)

        # Assume option moves ~ 0.5x of underlying percent move (ATM-ish)
        tgt_price = opt_entry * (1.0 + 0.5 * up_pct)
        sl_price  = opt_entry * (1.0 - 0.5 * dn_pct)
        sl_price  = _apply_sanity_sl(opt_entry, sl_price)

        rr = (tgt_price - opt_entry) / max(1e-6, (opt_entry - sl_price))

        out.append({
            "Timestamp": now_iso,
            "Symbol": sym,
            "UnderlyingType": "INDEX" if idx else "EQUITY",
            "UnderlyingPrice": round(entry, 2),
            "Exchange": "NSE",
            "Expiry": str(expiry),
            "Strike": strike,
            "Leg": leg,
            "Qty": 1,
            "EntryPrice": round(opt_entry, 2),
            "SL": round(sl_price, 2),
            "Target": round(tgt_price, 2),
            "RR": round(rr, 2),
            "OI": None, "IV": None,
            "Reason": f"synthetic {leg} ATM; step={step}"
        })

    df = pd.DataFrame(out)
    return df, src_tag
