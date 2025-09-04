# src/futures_executor.py
"""
NSE Futures executor with live fetch + safe fallback.

- Tries NSE derivatives quote JSON for INDEX & STOCK futures (nearest expiry).
- Builds simple paper entries (EntryPrice = lastPrice) with SL/Target rules.
- Applies minimal sanity (nonzero price). No leverage/risk sizing here.
- Falls back to synthetic if NSE blocks or data missing.
- Returns: (DataFrame, source_tag) where source_tag ∈ {'nse_live','synthetic'}

Output CSV columns:
Timestamp, Symbol, UnderlyingType, Exchange, Expiry,
EntryPrice, SL, Target, Lots, Reason
"""

from __future__ import annotations
import datetime as dt
from typing import Tuple, Dict, Any, List
import math
import time

import requests
import pandas as pd

from config import CONFIG

NSE_HOME = "https://www.nseindia.com"
# Futures quotes (indices + equities) are accessible via this instrument endpoint
# Format: https://www.nseindia.com/api/quote-derivative?symbol=<SYM>
DERIV_QUOTE = "https://www.nseindia.com/api/quote-derivative?symbol={sym}"

_DEFAULT_HEADERS = {
    "user-agent": ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
    "accept": "application/json, text/plain, */*",
    "referer": "https://www.nseindia.com",
    "cache-control": "no-cache",
    "pragma": "no-cache",
}

def _nse_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(_DEFAULT_HEADERS)
    try:
        s.get(NSE_HOME, timeout=8)
    except Exception:
        pass
    return s

def _normalize_symbol(sym: str) -> Tuple[str, str]:
    """
    Return (underlying_type, nse_symbol) where underlying_type ∈ {'INDEX','EQUITY'}.
    """
    s = str(sym).upper().replace(".NS","").replace(".BO","").strip()
    index_set = set(CONFIG.get("options", {}).get("indices", ["NIFTY","BANKNIFTY","FINNIFTY","MIDCPNIFTY"]))
    if s in index_set:
        return "INDEX", s
    return "EQUITY", s

def _fetch_fut_chain(sess: requests.Session, symbol: str) -> Dict[str, Any] | None:
    url = DERIV_QUOTE.format(sym=symbol)
    for _ in range(2):
        try:
            r = sess.get(url, timeout=12)
            if r.status_code == 200 and r.headers.get("content-type","").startswith("application/json"):
                return r.json()
        except Exception:
            pass
        time.sleep(0.8)
        try:
            sess.get(NSE_HOME, timeout=6)
        except Exception:
            pass
    return None

def _pick_nearest_expiry_fut(payload: Dict[str, Any]) -> Dict[str, Any] | None:
    """
    From quote-derivative JSON, pick nearest FUT instrument (INDEX/STOCK).
    The 'stocks' key (for equities) and 'underlyingValue' may vary; we rely on 'marketDeptOrderBook' lists.
    """
    if not isinstance(payload, dict):
        return None
    try:
        md = payload.get("marketDeptOrderBook", {})
        # derivatives lists could be like 'tradeable': ['FUTSTK','FUTIDX', ...]
        # But robustly, payload often has 'expiryDates' and 'selectedDate'.
        # The instrument data sits under 'marketDeptOrderBook' -> 'otherSeries' (sometimes), or
        # 'stocks' list with 'marketDeptOrderBook' per expiry. It's inconsistent.
        # Safer route: scan 'stocks' (equities) and 'marketDeptOrderBook' top-level (indices).
        # 1) try top-level 'marketDeptOrderBook' -> 'tradeable'
        series = payload.get("stocks") or []
        if not series and isinstance(md, dict) and md.get("tradeable"):
            # Index case: use 'data' list
            data = payload.get("derivatives", []) or payload.get("futures", []) or []
            # Some responses place instruments under 'marketDeptOrderBook' -> 'otherSeries'
            if not data:
                data = md.get("otherSeries", [])
            # pick earliest expiry
            best = None
            best_dt = None
            for ins in data:
                exp = ins.get("expiryDate")
                ltp = ins.get("lastPrice")
                if not exp or not ltp:
                    continue
                try:
                    dtp = dt.datetime.strptime(exp, "%d-%b-%Y").date()
                except Exception:
                    continue
                if best_dt is None or dtp < best_dt:
                    best_dt = dtp
                    best = ins
            return best

        # 2) equity case: iterate 'stocks' -> pick nearest expiry from 'marketDeptOrderBook' there
        best = None
        best_dt = None
        for row in series:
            mdb = row.get("marketDeptOrderBook", {})
            exp = row.get("expiryDate") or mdb.get("expiryDate")
            ltp = row.get("lastPrice") or mdb.get("lastPrice")
            if not exp or not ltp:
                continue
            try:
                dtp = dt.datetime.strptime(exp, "%d-%b-%Y").date()
            except Exception:
                continue
            if best_dt is None or dtp < best_dt:
                best_dt = dtp
                best = {"expiryDate": exp, "lastPrice": ltp}
        return best
    except Exception:
        return None

def _levels_from_ltp(ltp: float) -> Tuple[float, float]:
    # 1.5% stop, 1.5% target by default; tweak in CONFIG later
    return (ltp * 0.985, ltp * 1.015)

def _row(symbol: str, typ: str, expiry: str, ltp: float, lots: int, reason: str) -> Dict[str, Any]:
    sl, tgt = _levels_from_ltp(float(ltp))
    return {
        "Timestamp": dt.datetime.utcnow().isoformat()+"Z",
        "Symbol": symbol,
        "UnderlyingType": typ,          # 'INDEX' or 'EQUITY'
        "Exchange": "NSE",
        "Expiry": expiry,
        "EntryPrice": round(float(ltp), 2),
        "SL": round(sl, 2),
        "Target": round(tgt, 2),
        "Lots": int(lots),
        "Reason": reason or ""
    }

def _fallback_synthetic(symbol: str, typ: str, ref_price: float, lots: int, reason: str) -> Dict[str, Any]:
    entry = float(ref_price or 100.0)
    sl, tgt = _levels_from_ltp(entry)
    return {
        "Timestamp": dt.datetime.utcnow().isoformat()+"Z",
        "Symbol": symbol,
        "UnderlyingType": typ,
        "Exchange": "NSE",
        "Expiry": None,
        "EntryPrice": round(entry, 2),
        "SL": round(sl, 2),
        "Target": round(tgt, 2),
        "Lots": int(lots),
        "Reason": reason or "synthetic"
    }

def simulate_from_equity_recos(equity_df: pd.DataFrame) -> Tuple[pd.DataFrame, str]:
    """
    Prefer live NSE futures (nearest expiry); fallback to synthetic.
    equity_df columns used: Symbol, Entry, Reason
    Returns: (df, tag) with tag = 'nse_live' or 'synthetic'
    """
    cols = ["Timestamp","Symbol","UnderlyingType","Exchange","Expiry",
            "EntryPrice","SL","Target","Lots","Reason"]
    if equity_df is None or equity_df.empty:
        return pd.DataFrame(columns=cols), "synthetic"

    cfg = CONFIG.get("futures", {})
    allow_live = bool(cfg.get("enable_live_nse", True))
    lots = int(cfg.get("lot_size", 1))

    sess = _nse_session() if allow_live else None
    out: List[dict] = []
    used_live = False

    for r in equity_df.itertuples():
        typ, nse_sym = _normalize_symbol(str(getattr(r, "Symbol")))
        reason = str(getattr(r, "Reason", "")) or "equity_reco"
        ref = float(getattr(r, "Entry", 0.0) or 0.0)

        made = False
        if allow_live and sess is not None:
            payload = _fetch_fut_chain(sess, nse_sym)
            ins = _pick_nearest_expiry_fut(payload) if payload else None
            if ins and ins.get("lastPrice"):
                ltp = float(ins["lastPrice"])
                if ltp > 0:
                    exp = ins.get("expiryDate")
                    out.append(_row(nse_sym, typ, exp, ltp, lots, reason))
                    made = True
                    used_live = True

        if not made:
            out.append(_fallback_synthetic(nse_sym, typ, ref, lots, reason))

    return pd.DataFrame(out, columns=cols), ("nse_live" if used_live else "synthetic")
