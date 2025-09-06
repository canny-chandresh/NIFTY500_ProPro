from __future__ import annotations
import os, csv, datetime as dt
from typing import Dict, Any
from config import CONFIG
import broker_iface

def _append_csv(path: str, row: Dict[str, Any], header: list):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    file_exists = os.path.exists(path)
    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header)
        if not file_exists:
            w.writeheader()
        w.writerow(row)

def submit(symbol: str, side: str, qty: int, entry: float, sl: float, tp: float,
           book: str, mode_tag: str, meta: Dict[str, Any]):
    """
    mode_tag: 'PAPER' or 'LIVE'
    book: 'AUTO' | 'ALGO' | 'OPTIONS' | 'FUTURES'
    """
    ts = dt.datetime.utcnow().isoformat()+"Z"
    row = {
        "Timestamp": ts,
        "Book": book,
        "Mode": mode_tag,
        "Symbol": symbol,
        "Side": side,
        "Entry": entry,
        "SL": sl,
        "Target": tp,
        "Qty": qty,
        "Meta": str(meta)
    }

    target = {
        "AUTO": "datalake/paper_trades.csv" if mode_tag=="PAPER" else "datalake/live_trades.csv",
        "ALGO": "datalake/algo_paper.csv"   if mode_tag=="PAPER" else "datalake/algo_live.csv",
        "OPTIONS": "datalake/options_paper.csv",   # extend live later
        "FUTURES": "datalake/futures_paper.csv",   # extend live later
    }.get(book, "datalake/paper_trades.csv")

    header = ["Timestamp","Book","Mode","Symbol","Side","Entry","SL","Target","Qty","Meta"]
    _append_csv(target, row, header)

    # live route (only if LIVE and not dry_run)
    live_cfg = CONFIG.get("live", {})
    if mode_tag == "LIVE" and not bool(live_cfg.get("dry_run", True)):
        broker_iface.place_order(symbol, side, qty, entry, meta)
