# src/execution_simulator_v2.py
from __future__ import annotations
import json
from pathlib import Path
from typing import Dict

import numpy as np
import pandas as pd

def _paths(cfg: Dict):
    dl = Path(cfg.get("paths", {}).get("datalake","datalake"))
    rep = Path(cfg.get("paths", {}).get("reports","reports")) / "execution_v2"
    rep.mkdir(parents=True, exist_ok=True)
    return dl, rep

def _spread_regime(df: pd.DataFrame) -> float:
    vol = df["Close"].pct_change().tail(20).std()
    if vol < 0.01: return 5  # bps
    if vol < 0.02: return 10
    return 20

def _twap(prices: pd.Series, slices: int = 4) -> float:
    # equal-weight average across first N bars (approx intraday)
    if len(prices) < slices: return float(prices.mean())
    return float(prices.iloc[:slices].mean())

def simulate(cfg: Dict) -> Dict:
    dl, rep = _paths(cfg)
    p = dl / "paper_trades.csv"
    if not p.exists():
        (rep / "sim_status.json").write_text(json.dumps({"ok": True, "count": 0}), encoding="utf-8")
        return {"ok": True, "count": 0}

    pt = pd.read_csv(p, parse_dates=["timestamp"])
    if pt.empty:
        (rep / "sim_status.json").write_text(json.dumps({"ok": True, "count": 0}), encoding="utf-8")
        return {"ok": True, "count": 0}

    rows = []
    for _, r in pt.iterrows():
        sym = r["symbol"]
        ps = dl / "per_symbol" / f"{sym}.csv"
        if not ps.exists():
            continue
        df = pd.read_csv(ps)
        if "Close" not in df.columns: 
            continue
        # Regime-based spread
        bps = _spread_regime(df)
        # Entry TWAP of first 4 bars of next day (approx)
        prices = df["Close"]
        entry = _twap(prices.tail(10), 4)
        # Exit end-of-day (EOD) + slippage
        exitp = float(prices.iloc[-1])
        def slip(px, bps, side): 
            return px * (1 + (bps/10000.0) * (1 if side.upper()=="BUY" else -1))
        side = "BUY" if str(r.get("side","BUY")).upper()=="BUY" else "SELL"
        entry_s = slip(entry, bps, side)
        exit_s  = slip(exitp, bps, "SELL" if side=="BUY" else "BUY")
        qty = float(r.get("qty", 1))
        pnl = (exit_s - entry_s) * qty if side=="BUY" else (entry_s - exit_s) * qty
        rows.append({"symbol": sym, "entry": entry_s, "exit": exit_s, "pnl_realistic": pnl})

    out = pd.DataFrame(rows)
    out.to_csv(rep / "execution_realistic_v2.csv", index=False)
    summ = {
        "ok": True,
        "count": int(len(out)),
        "pnl_sum": float(out["pnl_realistic"].sum()) if len(out) else 0.0,
        "win_rate": float((out["pnl_realistic"]>0).mean()) if len(out) else 0.0
    }
    (rep / "sim_status.json").write_text(json.dumps(summ, indent=2), encoding="utf-8")
    return summ
