# src/backtester.py
from __future__ import annotations
import os, json, datetime as dt
import pandas as pd
from pathlib import Path
from config import CONFIG

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
    # bps fees on notional, plus flat SEBI fee once per trade
    bps_sum = (fee_cfg.get("commission_bps",0)+fee_cfg.get("stt_bps",0)+fee_cfg.get("exchange_bps",0)+fee_cfg.get("gst_bps",0))/1e4
    flat = float(fee_cfg.get("sebi_flat", 0.0))
    return pnl_gross - (notional*bps_sum) - flat

def backtest_trades(orders_df: pd.DataFrame, slippage_bps: float = 5.0, commission_bps: float = 1.0) -> dict:
    if orders_df is None or orders_df.empty:
        return {"trades": 0, "pnl": 0.0, "win_rate": None}
    d = orders_df.copy()
    d = d.dropna(subset=["Entry","Target","SL"])
    if d.empty:
        return {"trades": 0, "pnl": 0.0, "win_rate": None}

    slip = slippage_bps/1e4
    wins = (d["Target"] - d["Entry"]).abs() > (d["Entry"] - d["SL"]).abs()

    pnl_list = []; filled = []
    for i, r in d.iterrows():
        entry = float(r["Entry"]) * (1 + slip)
        target = float(r["Target"]) * (1 - slip)
        stop   = float(r["SL"])    * (1 - slip)
        engine = str(r.get("engine","EQUITY"))
        notional = float(r.get("size_pct", 0.2))*1_00_000  # assume base ₹1L for normalization

        # partial fill simulation (simplified): 90% fill if volatility high
        pf = 1.0 if float(r.get("rolling_vol", 0.02)) < 0.03 else 0.9
        ckt = _circuit_for(engine)
        # cap target/stop by circuit from entry
        max_up = entry*(1+ckt); max_dn = entry*(1-ckt)
        target = min(target, max_up); stop = max(stop, max_dn)

        win = bool(wins.loc[i]) if i in wins.index else False
        exit_px = target if win else stop
        pnl_gross = ((exit_px - entry)/entry)*notional*pf

        fee_cfg = _fees_for(engine)
        pnl_net = _apply_fees(pnl_gross, notional*pf, fee_cfg)

        pnl_list.append(pnl_net)
        filled.append(pf)

    d["pnl"] = pnl_list
    d["fill_ratio"] = filled
    summary = {
        "when_utc": dt.datetime.utcnow().isoformat()+"Z",
        "trades": int(len(d)),
        "wins": int((d["pnl"]>0).sum()),
        "win_rate": float((d["pnl"]>0).mean()*100.0),
        "pnl_sum": float(sum(pnl_list)),
        "pnl_mean": float(d["pnl"].mean()),
        "avg_fill": float(d["fill_ratio"].mean())
    }
    json.dump(summary, open(BT_DIR / f"bt_{dt.datetime.utcnow().strftime('%Y%m%d_%H%M%SZ')}.json","w"), indent=2)
    return summary

def walkforward_summary(history_df: pd.DataFrame) -> dict:
    from config import CONFIG
    if history_df is None or history_df.empty or "when_utc" not in history_df.columns:
        return {"windows": [], "overall": {}}
    df = history_df.copy()
    df["when_utc"] = pd.to_datetime(df["when_utc"], errors="coerce")
    df = df.dropna(subset=["when_utc"]).sort_values("when_utc")
    wins = (df["pnl"] > 0).astype(int)
    windows = CONFIG.get("walkforward",{}).get("windows",[30,90,252])
    res=[]
    for w in windows:
        sub=df.tail(w)
        if sub.empty: res.append({"window":w,"trades":0}); continue
        res.append({"window":w,"trades":int(len(sub)),"win_rate":float((sub["pnl"]>0).mean()*100.0),
                    "pnl_sum":float(sub["pnl"].sum()),"pnl_mean":float(sub["pnl"].mean())})
    overall={"trades":int(len(df)),"win_rate":float(wins.mean()*100.0 if len(df) else 0.0),
             "pnl_sum":float(df["pnl"].sum()),"pnl_mean":float(df["pnl"].mean())}
    payload={"when_utc":dt.datetime.utcnow().isoformat()+"Z","windows":res,"overall":overall}
    json.dump(payload, open(BT_DIR/"walkforward_summary.json","w"), indent=2)
    return payload
