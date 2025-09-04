from __future__ import annotations
import time, datetime as dt
from typing import Dict, Any, List
import requests
import pandas as pd
from config import CONFIG

NSE_HOME = "https://www.nseindia.com"
OC_IDX   = "https://www.nseindia.com/api/option-chain-indices?symbol={sym}"
OC_EQ    = "https://www.nseindia.com/api/option-chain-equities?symbol={sym}"

_HEADERS = {
    "user-agent": ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                   "KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
    "accept": "application/json, text/plain, */*",
    "referer": "https://www.nseindia.com",
    "cache-control": "no-cache",
    "pragma": "no-cache",
}

def _nse_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(_HEADERS)
    try: s.get(NSE_HOME, timeout=8)
    except Exception: pass
    return s

def _normalize(sym: str) -> tuple[str, str]:
    s = str(sym).upper().replace(".NS","").replace(".BO","").strip()
    idx = set(CONFIG.get("options",{}).get("indices",["NIFTY","BANKNIFTY","FINNIFTY","MIDCPNIFTY"]))
    return ("INDEX", s) if s in idx else ("EQUITY", s)

def _fetch_chain(sess: requests.Session, typ: str, symbol: str) -> Dict[str,Any] | None:
    url = OC_IDX.format(sym=symbol) if typ == "INDEX" else OC_EQ.format(sym=symbol)
    for _ in range(2):
        try:
            r = sess.get(url, timeout=12)
            if r.status_code == 200 and r.headers.get("content-type","").startswith("application/json"):
                return r.json()
        except Exception:
            pass
        time.sleep(0.8)
        try: sess.get(NSE_HOME, timeout=6)
        except Exception: pass
    return None

def _snap(symbol: str, typ: str, leg: str, strike: float, exp: str, ltp: float, reason: str) -> dict:
    sl  = ltp * 0.7
    tgt = ltp * 1.3
    rr  = (tgt - ltp) / max(1e-6, (ltp - sl))
    return {
        "Timestamp": dt.datetime.utcnow().isoformat()+"Z",
        "Symbol": symbol, "UnderlyingType": typ, "UnderlyingPrice": None,
        "Exchange":"NSE","Expiry":exp,"Strike":strike,"Leg":leg,
        "Qty":1, "EntryPrice":round(ltp,2), "SL":round(sl,2), "Target":round(tgt,2),
        "RR": round(rr,2), "OI": None, "IV": None, "Reason": reason
    }

def _pick_atm_nodes(payload: Dict[str,Any]) -> list[tuple[str,float,float,str]]:
    out = []
    if not payload or "records" not in payload: return out
    rec = payload["records"]
    data = rec.get("data") or []
    uv  = rec.get("underlyingValue") or 0.0

    # nearest strike ± one step
    uniq = sorted(list({r.get("strikePrice") for r in data if r.get("strikePrice") is not None}))
    if not uniq: return out
    nearest = min(uniq, key=lambda x: abs(x - uv))
    neigh = [nearest]
    i = uniq.index(nearest)
    if i-1 >= 0: neigh.append(uniq[i-1])
    if i+1 < len(uniq): neigh.append(uniq[i+1])

    expiries = rec.get("expiryDates") or []
    exp = expiries[0] if expiries else None
    for row in data:
        sp = row.get("strikePrice")
        if sp not in neigh: continue
        ce = (row.get("CE") or {})
        pe = (row.get("PE") or {})
        if exp and ce.get("lastPrice"): out.append((exp, float(sp), float(ce["lastPrice"]), "CE"))
        if exp and pe.get("lastPrice"): out.append((exp, float(sp), float(pe["lastPrice"]), "PE"))
    return out[:4]

def simulate_from_equity_recos(equity_df: pd.DataFrame):
    cols = ["Timestamp","Symbol","UnderlyingType","UnderlyingPrice","Exchange",
            "Expiry","Strike","Leg","Qty","EntryPrice","SL","Target","RR","OI","IV","Reason"]
    if equity_df is None or equity_df.empty:
        return pd.DataFrame(columns=cols), "synthetic"

    allow_live = bool(CONFIG.get("options",{}).get("enable_live_nse", True))
    ban = set(map(str.upper, CONFIG.get("options",{}).get("ban_list", [])))
    rr_min = float(CONFIG.get("options",{}).get("rr_min", 1.2))

    sess = _nse_session() if allow_live else None
    rows, used_live = [], False

    for r in equity_df.itertuples():
        typ, sym = _normalize(getattr(r,"Symbol"))
        if sym in ban: continue
        reason = f"equity_reco:{getattr(r,'Reason','')}"
        if allow_live and sess is not None:
            payload = _fetch_chain(sess, typ, sym)
            nodes = _pick_atm_nodes(payload) if payload else []
            for (exp, strike, ltp, leg) in nodes:
                row = _snap(sym, typ, leg, strike, exp, ltp, reason)
                if row["RR"] >= rr_min:
                    rows.append(row); used_live = True
        if not rows:
            rows.append(_snap(sym, typ, "CE", None, None, 5.0, "synthetic"))

    df = pd.DataFrame(rows, columns=cols)
    return df, ("nse_live" if used_live else "synthetic")

def train_from_live_chain(): 
    # Optional hook; no-op
    return True
