# src/broker_iface.py
from __future__ import annotations
import os, json, datetime as dt
from typing import Dict, Any

LOG = "reports/broker_stub.log"

def place_order(symbol: str, side: str, qty: int, price: float, meta: Dict[str, Any]) -> Dict[str, Any]:
    """
    SAFE STUB. Does not place real orders.
    Replace with Zerodha Kite connect integration later.
    """
    rec = {
        "when_utc": dt.datetime.utcnow().isoformat()+"Z",
        "symbol": symbol, "side": side, "qty": qty, "price": price,
        "meta": meta, "note": "stub_no_live_order"
    }
    os.makedirs(os.path.dirname(LOG) or ".", exist_ok=True)
    with open(LOG, "a") as f:
        f.write(json.dumps(rec) + "\n")
    return {"ok": True, "order_id": None, "stub": True, "recorded": rec}
