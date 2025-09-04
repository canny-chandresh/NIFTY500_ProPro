# src/options_executor.py
"""
NSE Options executor with live fetch + safe fallback.

- Tries live NSE option-chain for indices (NIFTY, BANKNIFTY, FINNIFTY, MIDCPNIFTY...)
  and equities (any reco symbol).
- Picks nearest weekly expiry, finds ATM CE/PE, uses LTP as entry, writes useful fields.
- Applies min RR gate (from CONFIG['options']['rr_min']).
- If NSE blocks or data is unavailable, falls back to a simple synthetic model.
- Returns: (DataFrame, source_tag) where source_tag ∈ {'nse_live','synthetic'}

CSV columns:
Timestamp, Symbol, UnderlyingType, UnderlyingPrice, Exchange, Expiry,
Strike, Leg, Qty, EntryPrice, SL, Target, RR, OI, IV, Reason
"""

from __future__ import annotations
import datetime as dt
import math
import time
from typing import Dict, List, Tuple

import pandas as pd
import requests

from config import CONFIG

# -------------------------
# NSE endpoints + headers
# -------------------------

NSE_HOME = "https://www.nseindia.com"
NSE_OC_EQ = "https://www.nseindia.com/api/option-chain-equities?symbol={sym}"   # equities
NSE_OC_IDX = "https://www.nseindia.com/api/option-chain-indices?symbol={sym}"   # indices

_DEFAULT_HEADERS = {
    "user-agent": ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
    "accept": "application/json, text/plain, */*",
    "referer": "https://www.nseindia.com/option-chain",
    "cache-control": "no-cache",
    "pragma": "no-cache",
}

def _nse_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(_DEFAULT_HEADERS)
    try:
        s.get(NSE_HOME, timeout=8)  # prime cookies
    except Exception:
        pass
    return s

def _normalize_symbol(sym: str) -> Tuple[str, str]:
    """Return (underlying_type, nse_symbol) where underlying_type in {'INDEX','EQUITY'}."""
    s = str(sym).upper().replace(".NS","").replace(".BO","").strip()
    index_set = set(CONFIG.get("options", {}).get("indices",
                    ["NIFTY","BANKNIFTY","FINNIFTY","MIDCPNIFTY"]))
    if s in index_set:
        return "INDEX", s
    return "EQUITY", s

def _extract_chain(json_payload: dict) -> Tuple[list, float]:
    if not isinstance(json_payload, dict):
        return [], float("nan")
    recs = json_payload.get("records", {})
    data = recs.get("data", [])
    under = recs.get("underlyingValue", None)
    try:
        under = float(under) if under is not None else float("nan")
    except Exception:
        under = float("nan")
    return data, under

def _nearest_weekly_expiry(records: list) -> str | None:
    expiries = set()
    for r in records:
        for d in (r.get("CE", {}), r.get("PE", {})):
            if isinstance(d, dict) and "expiryDate" in d:
                expiries.add(d["expiryDate"])
    if not expiries:
        return None
    def _parse(e):
        try: return dt.datetime.strptime(e, "%d-%b-%Y").date()
        except Exception: return dt.date.max
    return sorted(expiries, key=_parse)[0]

def _atm_strike(under_price: float, strikes: list[float]) -> float | None:
    if not strikes: return None
    return min(strikes, key=lambda k: abs(k - under_price))

def _fetch_chain(sess: requests.Session, underlying_type: str, symbol: str) -> Tuple[list, float]:
    url = NSE_OC_IDX.format(sym=symbol) if underlying_type == "INDEX" else NSE_OC_EQ.format(sym=symbol)
    for _ in range(2):  # light retry + cookie refresh
        try:
            r = sess.get(url, timeout=12)
            if r.status_code == 200 and r.headers.get("content-type","").startswith("application/json"):
                return _extract_chain(r.json())
        except Exception:
            pass
        time.sleep(0.8)
        try: sess.get(NSE_HOME, timeout=6)
        except Exception: pass
    return [], float("nan")

def _row(symbol: str, typ: str, under: float, expiry: str,
         strike: float, leg: str, qty: int, ltp: float, sl: float, tgt: float,
         reason: str, oi: float | None, iv: float | None) -> dict:
    rr = (abs(tgt - ltp) / max(1e-9, abs(ltp - sl))) if (ltp and sl and tgt) else None
    return {
        "Timestamp": dt.datetime.utcnow().isoformat()+"Z",
        "Symbol": symbol,
        "UnderlyingType": typ,            # 'INDEX' or 'EQUITY'
        "UnderlyingPrice": round(under, 2) if pd.notna(under) else None,
        "Exchange": "NSE",
        "Expiry": expiry,
        "Strike": round(strike, 2) if strike is not None else None,
        "Leg": leg,                       # "CE" / "PE"
        "Qty": int(qty),
        "EntryPrice": round(ltp, 2) if ltp is not None else None,
        "SL": round(sl, 2) if sl is not None else None,
        "Target": round(tgt, 2) if tgt is not None else None,
        "RR": round(rr, 2) if rr is not None else None,
        "OI": oi,
        "IV": iv,
        "Reason": reason or "",
    }

def _rr_ok(entry: float, sl: float, tgt: float, rr_min: float) -> bool:
    risk = abs(entry - sl)
    reward = abs(tgt - entry)
    if risk <= 1e-9: return False
    return (reward / risk) >= rr_min

# ---------- fallback synthetic (keeps runs green) ----------

def _fallback_synthetic(symbol: str, typ: str, under: float, reason: str, qty: int, rr_min: float) -> list[dict]:
    premium = max(1.0, 0.02 * float(under or 100.0))
    ce = (premium, premium*0.7, premium*1.6)
    pe = (premium, premium*0.7, premium*1.6)
    out = []
    if _rr_ok(*ce, rr_min): out.append(_row(symbol, typ, under, None, None, "CE", qty, *ce, reason, None, None))
    if _rr_ok(*pe, rr_min): out.append(_row(symbol, typ, under, None, None, "PE", qty, *pe, reason, None, None))
    return out

# ---------- public entry ----------

def simulate_from_equity_recos(equity_df: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    """
    Build options paper-trades (prefer live NSE chain; fallback to synthetic).
    equity_df columns used: Symbol, Entry, SL, Target, proba, Reason
    Returns: (df, source_tag) where source_tag is 'nse_live' or 'synthetic'.
    """
    cols = ["Timestamp","Symbol","UnderlyingType","UnderlyingPrice","Exchange",
            "Expiry","Strike","Leg","Qty","EntryPrice","SL","Target","RR","OI","IV","Reason"]
    if equity_df is None or equity_df.empty:
        return pd.DataFrame(columns=cols), "synthetic"

    cfg = CONFIG.get("options", {})
    rr_min = float(cfg.get("rr_min", 1.2))
    lot    = int(cfg.get("lot_size", 1))
    ban    = set([s.upper() for s in cfg.get("ban_list", [])])
    live   = bool(cfg.get("enable_live_nse", True))

    sess = _nse_session() if live else None
    rows: list[dict] = []
    used_live = False

    for r in equity_df.itertuples():
        sym_raw = str(getattr(r, "Symbol"))
        if sym_raw.upper() in ban: 
            continue

        typ, nse_sym = _normalize_symbol(sym_raw)
        reason = str(getattr(r, "Reason", "")) or "equity_reco"
        under_guess = float(getattr(r, "Entry", 0.0) or 0.0)

        made = False
        if live and sess is not None:
            chain, underlying = _fetch_chain(sess, typ, nse_sym)
            if chain:
                strikes = sorted({ float(x.get("strikePrice", "nan")) for x in chain if "strikePrice" in x })
                under = underlying if (underlying and not math.isnan(underlying)) else under_guess
                strike = _atm_strike(under, strikes)
                expiry = _nearest_weekly_expiry(chain)

                ce_ltp = pe_ltp = None
                ce_oi = pe_oi = None
                ce_iv = pe_iv = None
                for rec in chain:
                    if float(rec.get("strikePrice", -1)) != strike:
                        continue
                    CE = rec.get("CE", {})
                    if CE and CE.get("expiryDate") == expiry:
                        ce_ltp = CE.get("lastPrice"); ce_oi = CE.get("openInterest"); ce_iv = CE.get("impliedVolatility")
                    PE = rec.get("PE", {})
                    if PE and PE.get("expiryDate") == expiry:
                        pe_ltp = PE.get("lastPrice"); pe_oi = PE.get("openInterest"); pe_iv = PE.get("impliedVolatility")

                def st_levels(ltp: float) -> tuple[float,float]:
                    return (ltp * 0.7, ltp * 1.6)  # 30% stop, 60% target

                if ce_ltp and ce_ltp > 0:
                    sl, tgt = st_levels(ce_ltp)
                    if _rr_ok(ce_ltp, sl, tgt, rr_min):
                        rows.append(_row(nse_sym, typ, under, expiry, strike, "CE", lot, ce_ltp, sl, tgt, reason, ce_oi, ce_iv))
                        made = True
                if pe_ltp and pe_ltp > 0:
                    sl, tgt = st_levels(pe_ltp)
                    if _rr_ok(pe_ltp, sl, tgt, rr_min):
                        rows.append(_row(nse_sym, typ, under, expiry, strike, "PE", lot, pe_ltp, sl, tgt, reason, pe_oi, pe_iv))
                        made = True

                used_live = used_live or made

        if not made:
            rows.extend(_fallback_synthetic(nse_sym, typ, under_guess, reason, lot, rr_min))

    tag = "nse_live" if used_live else "synthetic"
    return pd.DataFrame(rows, columns=cols), tag
