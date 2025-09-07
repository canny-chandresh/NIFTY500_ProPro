# src/options_live_nse.py
from __future__ import annotations
import json, datetime as dt, time
from typing import Dict, Any
import pandas as pd

# network import guarded so offline runs don't fail
def _requests():
    import requests
    return requests

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.nseindia.com/",
    "Connection": "keep-alive",
}
BASE = "https://www.nseindia.com/api/option-chain-indices?symbol={symbol}"

def _synthetic_payload(symbol: str) -> Dict[str, Any]:
    """Simple, deterministic synthetic chain snapshot (as fallback)."""
    now = dt.datetime.utcnow()
    strikes = [i for i in range(20000, 20550, 50)] if symbol.upper()=="NIFTY" else [i for i in range(44000, 44600, 100)]
    rows = []
    for k in strikes:
        rows.append({"type":"CE","strike":k,"exp":"SYN","iv":18.0,"oi":10000,"ltp":120.0,"underlying":symbol})
        rows.append({"type":"PE","strike":k,"exp":"SYN","iv":19.5,"oi":9000,"ltp":110.0,"underlying":symbol})
    df = pd.DataFrame.from_records(rows)
    return {"ok": True, "timestamp": now.isoformat()+"Z", "symbol": symbol, "rows": len(df), "df": df, "source":"synthetic"}

def fetch_index_option_chain(symbol: str = "NIFTY", timeout: int = 10) -> Dict[str, Any]:
    """Polite NSE fetch; falls back to synthetic on any block/error."""
    try:
        requests = _requests()
        s = requests.Session(); s.headers.update(HEADERS)
        r = s.get(BASE.format(symbol=symbol.upper()), timeout=timeout)
        if r.status_code != 200:
            return _synthetic_payload(symbol)
        data = r.json()
        records = []
        for rec in data.get("records", {}).get("data", []):
            ce = rec.get("CE"); pe = rec.get("PE")
            if ce:
                records.append({"type":"CE","strike":ce.get("strikePrice"),"exp":ce.get("expiryDate"),
                                "iv":ce.get("impliedVolatility"),"oi":ce.get("openInterest"),
                                "ltp":ce.get("lastPrice"),"underlying":ce.get("underlying")})
            if pe:
                records.append({"type":"PE","strike":pe.get("strikePrice"),"exp":pe.get("expiryDate"),
                                "iv":pe.get("impliedVolatility"),"oi":pe.get("openInterest"),
                                "ltp":pe.get("lastPrice"),"underlying":pe.get("underlying")})
        df = pd.DataFrame.from_records(records)
        ts = dt.datetime.utcnow().isoformat()+"Z"
        return {"ok": True, "timestamp": ts, "symbol": symbol.upper(), "rows": len(df), "df": df, "source":"nse"}
    except Exception:
        return _synthetic_payload(symbol)

def to_parquet(payload, path: str):
    if not payload.get("ok"): return False
    df = payload["df"].copy()
    df["fetched_utc"] = payload.get("timestamp")
    df["source"] = payload.get("source","unknown")
    df.to_parquet(path, index=False)
    return True
