# src/backtester.py
from __future__ import annotations
import os, json, datetime as dt
import pandas as pd
from pathlib import Path

BT_DIR = Path("reports/backtests")
BT_DIR.mkdir(parents=True, exist_ok=True)

"""
Very simple trade-level backtester with slippage & commission.
Inputs: paper_trades.csv-like DF (Entry, Target, SL, when_utc, Symbol, mode)
- Assumes 'long' logic: wins if Target reached before SL; else loss.
- Slippage applied on both entry and exit.
- Commission (bps) applied on notional.
Writes summary json per run.
"""

def backtest_trades(
    orders_df: pd.DataFrame,
    slippage_bps: float = 5.0,
    commission_bps: float = 1.0,
    lookahead_bars: int = 20
) -> dict:
    if orders_df is None or orders_df.empty:
        return {"trades": 0, "pnl": 0.0, "win_rate": None}

    d = orders_df.copy()
    d = d.dropna(subset=["Entry","Target","SL"])
    if d.empty:
        return {"trades": 0, "pnl": 0.0, "win_rate": None}

    # Simple outcome simulation: if Target-Entry > Entry-SL assume prob win ~ proba else loss
    # (Replace with real bar-by-bar check when you wire OHLCV per-symbol.)
    wins = (d["Target"] - d["Entry"]).abs() > (d["Entry"] - d["SL"]).abs()
    # PnL per trade with slippage & commission (bps -> fraction)
    slip = slippage_bps / 1e4
    com  = commission_bps / 1e4
    entry = d["Entry"] * (1 + slip)
    # Exit price
    exit_px = d["Target"].where(wins, d["SL"])
    exit_px = exit_px * (1 - slip)
    pnl = (exit_px - entry) / entry
    pnl = pnl - com  # commission
    d["pnl"] = pnl
    summary = {
        "when_utc": dt.datetime.utcnow().isoformat()+"Z",
        "trades": int(len(d)),
        "wins": int(wins.sum()),
        "win_rate": float((wins.mean()*100.0) if len(d) else 0.0),
        "pnl_sum": float(d["pnl"].sum()),
        "pnl_mean": float(d["pnl"].mean()),
        "slippage_bps": slippage_bps,
        "commission_bps": commission_bps
    }
    out = BT_DIR / f"bt_{dt.datetime.utcnow().strftime('%Y%m%d_%H%M%SZ')}.json"
    json.dump(summary, open(out,"w"), indent=2)
    return summary
