# src/backtester.py
from __future__ import annotations
import os, json, datetime as dt
import pandas as pd
from pathlib import Path
from config import CONFIG
from eligibility import load_lot_tick, load_liquidity

BT_DIR = Path("reports/backtests"); BT_DIR.mkdir(parents=True, exist_ok=True)

def _fees_for(engine: str) -> dict:
    e = str(engine).upper()
    if "OPTION" in e:   return CONFIG["fees"]["options"]
    if "FUTURE" in e:   return CONFIG["fees"]["futures"]
    return CONFIG["fees"]["equity"]

def _circuit_for(engine: str) -> float:
    e = str(engine).upper()
    if "OPTION" in e or "FUTURE" in e: return float(CONFIG["market"]["fno_circuit_pct"])
    return float(CONFIG["market"]["equity_circuit_pct"])

def _apply_fees(pnl_gross: float, notional: float, fee_cfg: dict) -> float:
    bps_sum = (fee_cfg.get("commission_bps",0)+fee_cfg.get("stt_bps",0)+fee_cfg.get("exchange_bps",0)+fee_cfg.get("gst_bps",0))/1e4
    flat = float(fee_cfg.get("sebi_flat", 0.0))
    return pnl_gross - (notional*bps_sum) - flat

def _round_tick(px: float, tick: float) -> float:
    if tick <= 0: return float(px)
    return round(px / tick) * tick

def _impact_bp_from_adv(notional: float, adv_value: float) -> float:
    """
    Square-root impact: impact_bps ~ c * sqrt(notional / ADV), c ~= 40 bps for small caps
    Calibrate conservatively for free stack.
    """
    if adv_value <= 0: return 80.0
    ratio = max(0.0, min(1.0, notional / adv_value))
    return 40.0 * (ratio ** 0.5)  # bps

def backtest_trades(orders_df: pd.DataFrame, slippage_bps: float = 5.0, commission_bps: float = 1.0) -> dict:
    if orders_df is None or orders_df.empty:
        return {"trades": 0, "pnl": 0.0, "win_rate": None}
    d = orders_df.copy()
    d = d.dropna(subset=["Entry","Target","SL"])
    if d.empty:
        return {"trades": 0, "pnl": 0.0, "win_rate": None}

    lottick = load_lot_tick()    # Symbol, lot_size, tick_size
    liq = load_liquidity()       # Symbol, adv_value
    d = d.merge(lottick[["Symbol","lot_size","tick_size"]], on="Symbol", how="left")
    d = d.merge(liq, on="Symbol", how="left")
    d["lot_size"]  = d["lot_size"].fillna(1).astype(int)
    d["tick_size"] = d["tick_size"].fillna(0.05)
    d["adv_value"] = d["adv_value"].fillna(2_00_00_000)

    slip = slippage_bps/1e4
    pnl_list=[]; filled=[]
    for i, r in d.iterrows():
        entry = float(r["Entry"]) * (1 + slip)
        tick  = float(r["tick_size"])
        entry = _round_tick(entry, tick)
        target = _round_tick(float(r["Target"]), tick)
        stop   = _round_tick(float(r["SL"]), tick)
        engine = str(r.get("engine","EQUITY"))
        notional = float(r.get("size_pct", 0.2))*1_00_000
        lots = max(1, int(round(notional / max(1.0, entry) / max(1, int(r["lot_size"])))))  # crude
        notional = lots * entry * max(1, int(r["lot_size"]))

        # impact bps by ADV bucket
        imp_bps = _impact_bp_from_adv(notional, float(r["adv_value"])) / 1e4
        entry *= (1 + imp_bps)

        ckt = _circuit_for(engine); max_up = entry*(1+ckt); max_dn = entry*(1-ckt)
        target = min(target, max_up); stop = max(stop, max_dn)

        win = abs(target-entry) >= abs(entry-stop)
        exit_px = target if win else stop
        pnl_gross = (exit_px - entry) * lots * max(1, int(r["lot_size"]))

        fee_cfg = _fees_for(engine)
        pnl_net = _apply_fees(pnl_gross, notional, fee_cfg)

        pnl_list.append(pnl_net)
        filled.append(1.0 if imp_bps<0.005 else 0.9)

    d["pnl"]=pnl_list; d["fill_ratio"]=filled
    summary = {
        "when_utc": dt.datetime.utcnow().isoformat()+"Z",
        "trades": int(len(d)),
        "wins": int((d["pnl"]>0).sum()),
        "win_rate": float((d["pnl"]>0).mean()*100.0),
        "pnl_sum": float(sum(pnl_list)),
        "pnl_mean": float(d["pnl"].mean()),
        "avg_fill": float(d["fill_ratio"].mean())
    }
    (BT_DIR / f"bt_{dt.datetime.utcnow().strftime('%Y%m%d_%H%M%SZ')}.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary
