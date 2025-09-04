from __future__ import annotations
import requests, time, datetime as dt
from typing import Dict, Any
import pandas as pd
from config import CONFIG

NSE_HOME = "https://www.nseindia.com"
DERIV_QUOTE = "https://www.nseindia.com/api/quote-derivative?symbol={sym}"

_HEADERS = {
    "user-agent": ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                   "KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
    "accept": "application/json, text/plain, */*",
    "referer": "https://www.nseindia.com",
    "cache-control": "no-cache",
    "pragma": "no-cache",
}

def _nse_session():
    s = requests.Session(); s.headers.update(_HEADERS)
    try: s.get(NSE_HOME, timeout=8)
    except Exception: pass
    return s

def _normalize(sym: str) -> tuple[str,str]:
    s = str(sym).upper().replace(".NS","").replace(".BO","").strip()
    idx = set(CONFIG.get("options",{}).get("indices",["NIFTY","BANKNIFTY","FINNIFTY","MIDCPNIFTY"]))
    return ("INDEX", s) if s in idx else ("EQUITY", s)

def _fetch(sess, sym: str):
    url = DERIV_QUOTE.format(sym=sym)
    for _ in range(2):
        try:
            r = sess.get(url, timeout=12)
            if r.status_code==200 and r.headers.get("content-type","").startswith("application/json"):
                return r.json()
        except Exception:
            pass
        time.sleep(0.8)
        try: sess.get(NSE_HOME, timeout=6)
        except Exception: pass
    return None

def _nearest(payload) -> dict|None:
    try:
        md = payload.get("marketDeptOrderBook", {})
        data = payload.get("derivatives", []) or payload.get("futures", []) or md.get("otherSeries", [])
        best, best_dt = None, None
        for ins in data:
            exp = ins.get("expiryDate"); ltp = ins.get("lastPrice")
            if not exp or not ltp: continue
            dtp = dt.datetime.strptime(exp, "%d-%b-%Y").date()
            if best_dt is None or dtp < best_dt: best_dt, best = dtp, ins
        return best
    except Exception:
        return None

def _levels(ltp: float):
    return (ltp*0.985, ltp*1.015)

def _row(sym, typ, exp, ltp, lots, reason):
    sl, tgt = _levels(float(ltp))
    return {
        "Timestamp": dt.datetime.utcnow().isoformat()+"Z",
        "Symbol": sym, "UnderlyingType": typ, "Exchange":"NSE", "Expiry": exp,
        "EntryPrice": round(float(ltp),2), "SL": round(sl,2), "Target": round(tgt,2),
        "Lots": int(lots), "Reason": reason
    }

def _fallback(sym, typ, ref, lots, reason):
    entry = float(ref or 100.0); sl, tgt = _levels(entry)
    return {
        "Timestamp": dt.datetime.utcnow().isoformat()+"Z",
        "Symbol": sym, "UnderlyingType": typ, "Exchange":"NSE", "Expiry": None,
        "EntryPrice": round(entry,2), "SL": round(sl,2), "Target": round(tgt,2),
        "Lots": int(lots), "Reason": reason or "synthetic"
    }

def simulate_from_equity_recos(equity_df: pd.DataFrame):
    cols = ["Timestamp","Symbol","UnderlyingType","Exchange","Expiry","EntryPrice","SL","Target","Lots","Reason"]
    if equity_df is None or equity_df.empty:
        return pd.DataFrame(columns=cols), "synthetic"

    cfg = CONFIG.get("futures", {})
    allow_live = bool(cfg.get("enable_live_nse", True))
    lots = int(cfg.get("lot_size", 1))

    sess = _nse_session() if allow_live else None
    out, used_live = [], False

    for r in equity_df.itertuples():
        typ, sym = _normalize(str(getattr(r,"Symbol")))
        reason = f"equity_reco:{getattr(r,'Reason','')}"
        ref = float(getattr(r,"Entry", 0.0) or 0.0)

        made = False
        if allow_live and sess is not None:
            pl = _fetch(sess, sym)
            ins = _nearest(pl) if pl else None
            if ins and ins.get("lastPrice"):
                out.append(_row(sym, typ, ins.get("expiryDate"), float(ins["lastPrice"]), lots, reason))
                made, used_live = True, True
        if not made:
            out.append(_fallback(sym, typ, ref, lots, reason))

    return pd.DataFrame(out, columns=cols), ("nse_live" if used_live else "synthetic")

def train_from_live_futures():
    # Optional hook; no-op
    return True
