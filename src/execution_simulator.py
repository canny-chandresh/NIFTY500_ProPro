# src/execution_simulator.py
from __future__ import annotations
import json
from pathlib import Path
from typing import Dict

import numpy as np
import pandas as pd

def _paths(cfg: Dict):
    dl = Path(cfg.get("paths", {}).get("datalake", "datalake"))
    rep = Path(cfg.get("paths", {}).get("reports", "reports"))
    rep_exec = rep / "execution"
    rep_exec.mkdir(parents=True, exist_ok=True)
    return dl, rep_exec

def _realism(cfg: Dict) -> Dict:
    return cfg.get("realism", {
        "slippage_open_bps": 30,
        "slippage_mid_bps": 10,
        "slippage_close_bps": 20
    })

def _load_paper(dl: Path) -> pd.DataFrame:
    p = dl / "paper_trades.csv"
    if p.exists():
        return pd.read_csv(p, parse_dates=["timestamp"])
    return pd.DataFrame(columns=["timestamp","symbol","side","price","qty","engine"])

def _approx_close(dl: Path, sym: str) -> float:
    # Very rough: look into per_symbol last close if available
    ps = dl / "per_symbol" / f"{sym}.csv"
    if ps.exists():
        try:
            df = pd.read_csv(ps)
            if "Close" in df.columns and len(df):
                return float(df["Close"].iloc[-1])
        except Exception:
            pass
    return np.nan

def _apply_slippage(px: float, bps: int, side: str) -> float:
    # BUY pays more, SELL receives less
    adj = px * (bps / 10000.0)
    return px + adj if side.upper() == "BUY" else px - adj

def simulate(cfg: Dict) -> Dict:
    dl, rep_exec = _paths(cfg)
    rlz = _realism(cfg)

    trades = _load_paper(dl)
    if trades.empty:
        (rep_exec / "sim_status.json").write_text(json.dumps({"ok": True, "count": 0}), encoding="utf-8")
        return {"ok": True, "count": 0}

    rows = []
    for _, r in trades.iterrows():
        sym = r["symbol"]
        side = str(r.get("side","BUY")).upper()
        px_entry = float(r.get("price", 0.0))
        px_exit = _approx_close(dl, sym)
        if np.isnan(px_exit):
            # assume flat EOD at entry for now
            px_exit = px_entry

        # Apply slippage to entry & exit
        px_e_slip = _apply_slippage(px_entry, int(rlz.get("slippage_open_bps", 30)), side)
        px_x_slip = _apply_slippage(px_exit, int(rlz.get("slippage_close_bps", 20)), "SELL" if side=="BUY" else "BUY")
        qty = float(r.get("qty", 1))
        pnl = (px_x_slip - px_e_slip) * qty if side == "BUY" else (px_e_slip - px_x_slip) * qty

        rows.append({
            "timestamp": r["timestamp"],
            "symbol": sym,
            "side": side,
            "entry_price": px_entry,
            "entry_slipped": px_e_slip,
            "exit_price": px_exit,
            "exit_slipped": px_x_slip,
            "qty": qty,
            "pnl_realistic": pnl,
            "engine": r.get("engine","mix"),
        })

    out = pd.DataFrame(rows)
    out.to_csv(rep_exec / "execution_realistic.csv", index=False)

    summary = {
        "ok": True,
        "count": int(len(out)),
        "pnl_sum": float(out["pnl_realistic"].sum()),
        "pnl_mean": float(out["pnl_realistic"].mean()),
        "win_rate": float((out["pnl_realistic"] > 0).mean()) if len(out) else 0.0
    }
    (rep_exec / "sim_status.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary
