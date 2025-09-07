# src/options_live_nse.py
from __future__ import annotations
import time, json, datetime as dt
from typing import Dict, Any
import pandas as pd
import requests

NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.nseindia.com/",
    "Connection": "keep-alive",
}

BASE = "https://www.nseindia.com/api/option-chain-indices?symbol={symbol}"

def fetch_index_option_chain(symbol: str = "NIFTY") -> Dict[str, Any]:
    """
    Light-touch NSE options chain fetch.
    IMPORTANT:
      - Keep frequency low (≥ 15 minutes).
      - Do not hammer endpoints; respect robots & terms.
      - If blocked (HTTP 401/403), backoff and use synthetic fallback.
    """
    url = BASE.format(symbol=symbol.upper())
    s = requests.Session()
    s.headers.update(NSE_HEADERS)
    try:
        r = s.get(url, timeout=10)
        if r.status_code != 200:
            return {"ok": False, "status": r.status_code, "reason": "blocked_or_error"}
        data = r.json()
        # compact to a DataFrame
        records = []
        for rec in data.get("records", {}).get("data", []):
            ce = rec.get("CE"); pe = rec.get("PE")
            if ce:
                records.append({
                    "type":"CE","strike": ce.get("strikePrice"), "exp": ce.get("expiryDate"),
                    "iv": ce.get("impliedVolatility"), "oi": ce.get("openInterest"),
                    "ltp": ce.get("lastPrice"), "underlying": ce.get("underlying"),
                })
            if pe:
                records.append({
                    "type":"PE","strike": pe.get("strikePrice"), "exp": pe.get("expiryDate"),
                    "iv": pe.get("impliedVolatility"), "oi": pe.get("openInterest"),
                    "ltp": pe.get("lastPrice"), "underlying": pe.get("underlying"),
                })
        df = pd.DataFrame.from_records(records)
        ts = dt.datetime.utcnow().isoformat()+"Z"
        return {"ok": True, "timestamp": ts, "symbol": symbol.upper(), "rows": len(df), "df": df}
    except Exception as e:
        return {"ok": False, "reason": repr(e)}

def to_parquet(payload, path: str):
    if not payload.get("ok"): return False
    df = payload["df"].copy()
    df["fetched_utc"] = payload.get("timestamp")
    df.to_parquet(path, index=False)
    return True
