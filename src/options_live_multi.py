"""
Multi-provider options fetcher.
Default: NSE option chain.
Fallback: synthetic generator.
"""

import requests, pandas as pd, numpy as np
from datetime import datetime

def fetch_options(symbol="NIFTY", expiry=None):
    url = f"https://www.nseindia.com/api/option-chain-indices?symbol={symbol}"
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(url, headers=headers, timeout=10)
        data = r.json()
        records = data["records"]["data"]
        rows = []
        for rec in records:
            ce, pe = rec.get("CE"), rec.get("PE")
            if ce: rows.append(_parse(ce,"CE"))
            if pe: rows.append(_parse(pe,"PE"))
        return pd.DataFrame(rows)
    except Exception as e:
        print(f"[options_live_multi] NSE fetch failed, fallback: {e}")
        return _synthetic(symbol)

def _parse(o, typ):
    return {
        "strike": o["strikePrice"],
        "type": typ,
        "iv": o.get("impliedVolatility", np.nan),
        "oi": o.get("openInterest", np.nan),
        "ltp": o.get("lastPrice", np.nan),
        "source": "nse",
        "fetched_utc": datetime.utcnow().isoformat()+"Z"
    }

def _synthetic(symbol):
    return pd.DataFrame([{
        "strike":10000,"type":"CE","iv":20,"oi":100,"ltp":100,
        "source":"synthetic","fetched_utc":datetime.utcnow().isoformat()+"Z"}])
