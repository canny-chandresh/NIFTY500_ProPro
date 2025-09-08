# src/backtest_harness_v2.py
from __future__ import annotations
import json, math, itertools
from pathlib import Path
from typing import Dict, List, Callable, Tuple

import numpy as np
import pandas as pd

def _paths(cfg: Dict):
    rep = Path(cfg.get("paths", {}).get("reports", "reports"))
    out = rep / "backtests_v2"
    out.mkdir(parents=True, exist_ok=True)
    return rep, out

def _universe(cfg: Dict, per_dir: str) -> List[str]:
    uu = cfg.get("universe", {}).get("symbols", [])
    if uu: return uu
    p = Path(per_dir)
    return sorted([x.stem for x in p.glob("*.csv")])[:200]

def _fees(cfg: Dict) -> Dict:
    return cfg.get("fees", {"bps": 3.0})

def _slip(cfg: Dict) -> int:
    return int(cfg.get("realism", {}).get("slippage_mid_bps", 10))

def _pos_sizing(cfg: Dict) -> str:
    return cfg.get("risk", {}).get("position_size", "fixed1")  # fixed1 / vol_target

def _calc_size(px: float, vol: float, mode: str) -> float:
    if mode == "vol_target":
        target_risk = 0.01  # 1% per trade
        denom = max(1e-6, vol)
        return float(max(1.0, (target_risk / denom)))
    return 1.0

def _load_symbol(per_dir: str, sym: str) -> pd.DataFrame:
    p = Path(per_dir) / f"{sym}.csv"
    if not p.exists(): return pd.DataFrame()
    df = pd.read_csv(p, parse_dates=True)
    # normalize
    if "Date" in df.columns: df = df.set_index("Date")
    df.index = pd.to_datetime(df.index)
    return df[["Open","High","Low","Close","Volume"]].dropna()

def _apply_slippage(px: float, bps: int, side: str) -> float:
    adj = px * (bps/10000.0)
    return px + adj if side.upper()=="BUY" else px - adj

def _apply_fees(pnl: float, fees_bps: float, notional: float) -> float:
    return pnl - (notional * (fees_bps/10000.0))

def _signals_from_scores(scores: pd.Series, k: int) -> List[int]:
    # top-k -> +1; bottom-k -> -1; others 0 (long/short test)
    order = scores.sort_values(ascending=False)
    long_syms  = set(order.head(k).index)
    short_syms = set(order.tail(k).index)
    sigs = []
    for s in scores.index:
        sigs.append( 1 if s in long_syms else (-1 if s in short_syms else 0) )
    return sigs

def _walk(df_dict: Dict[str, pd.DataFrame], cfg: Dict, params: Dict) -> Dict:
    # params: k, stop_pct, target_pct, trail_pct
    k           = int(params.get("k", 5))
    stop_pct    = float(params.get("stop_pct", 0.025))
    target_pct  = float(params.get("target_pct", 0.05))
    trail_pct   = float(params.get("trail_pct", 0.03))
    slip_bps    = _slip(cfg)
    fees_bps    = float(_fees(cfg)["bps"])
    size_mode   = _pos_sizing(cfg)

    dates = sorted(set.intersection(*[set(df.index) for df in df_dict.values()]))
    daily_pnl = []
    trades = []

    # For simplicity, we’ll rank by yesterday’s return momentum
    def day_score(df: pd.DataFrame, t: pd.Timestamp) -> float:
        # use last 5d momentum scaled by vol
        w = df.loc[:t].tail(6)
        if len(w)<6: return 0.0
        ret = (w["Close"].pct_change().tail(5)).sum()
        vol = w["Close"].pct_change().tail(20).std() or 1e-6
        return float(ret / vol)

    for i in range(21, len(dates)-1):
        t = dates[i]
        t1 = dates[i+1]  # trade day exit end-of-day
        # rank scores at t
        scores = {}
        for s, df in df_dict.items():
            if t not in df.index: continue
            scores[s] = day_score(df, t)
        if not scores: 
            daily_pnl.append(0.0); 
            continue

        sr = pd.Series(scores).dropna()
        sigs = _signals_from_scores(sr, k)

        entry_pnl = 0.0
        for sym, sig in zip(sr.index, sigs):
            if sig == 0: 
                continue
            df = df_dict[sym]
            if t not in df.index or t1 not in df.index:
                continue
            opx = float(df.at[t1, "Open"])   # enter next open
            clx = float(df.at[t1, "Close"])  # exit end of day

            # apply bracket logic intra-day approx via Hi/Lo
            hi = float(df.at[t1, "High"]); lo = float(df.at[t1, "Low"])
            entry = _apply_slippage(opx, slip_bps, "BUY" if sig>0 else "SELL")
            size  = _calc_size(opx, df["Close"].pct_change().tail(20).std() or 1e-6, size_mode)

            # target/stop simulation (rough)
            tgt = entry * (1.0 + target_pct * (1 if sig>0 else -1))
            stp = entry * (1.0 - stop_pct   * (1 if sig>0 else -1))
            exit_px = clx
            if sig>0:
                if hi >= tgt: exit_px = tgt
                elif lo <= stp: exit_px = stp
            else:
                if lo <= tgt: exit_px = tgt
                elif hi >= stp: exit_px = stp

            exit_px = _apply_slippage(exit_px, slip_bps, "SELL" if sig>0 else "BUY")
            raw = (exit_px - entry) * size * (1 if sig>0 else -1)
            notional = abs(entry * size)
            pnl = _apply_fees(raw, fees_bps, notional)
            entry_pnl += pnl

            trades.append({
                "date": t1, "symbol": sym, "side": "LONG" if sig>0 else "SHORT",
                "entry": entry, "exit": exit_px, "size": size, "pnl": pnl
            })

        daily_pnl.append(entry_pnl)

    res = pd.DataFrame({"date": dates[21:len(dates)-1], "pnl": daily_pnl})
    return {"daily": res, "trades": pd.DataFrame(trades)}

def run(cfg: Dict, param_grid: Dict = None) -> Dict:
    rep, out = _paths(cfg)
    if param_grid is None:
        param_grid = {"k":[3,5,7], "stop_pct":[0.02,0.025,0.03], "target_pct":[0.04,0.05,0.06]}

    dl = cfg.get("paths", {}).get("datalake","datalake")
    per_dir = cfg.get("paths", {}).get("per_symbol", f"{dl}/per_symbol")
    syms = _universe(cfg, per_dir)[:60]

    df_dict = {s:_load_symbol(per_dir, s) for s in syms}
    df_dict = {k:v for k,v in df_dict.items() if not v.empty}
    if not df_dict:
        (out / "status.json").write_text(json.dumps({"ok": False, "reason":"no_data"}), encoding="utf-8")
        return {"ok": False, "reason":"no_data"}

    grid = list(itertools.product(*[param_grid[k] for k in param_grid]))
    rows = []
    for combo in grid:
        params = {k: combo[i] for i,k in enumerate(param_grid)}
        sim = _walk(df_dict, cfg, params)
        d = sim["daily"]; tr = sim["trades"]
        pnl = float(d["pnl"].sum())
        sharpe = float(np.mean(d["pnl"]) / (np.std(d["pnl"])+1e-9) * np.sqrt(252)) if len(d)>5 else 0.0
        wr = float((tr["pnl"]>0).mean()) if len(tr)>0 else 0.0
        rows.append({**params, "pnl": pnl, "sharpe": sharpe, "win_rate": wr, "trades": int(len(tr))})

    lb = pd.DataFrame(rows).sort_values(["sharpe","pnl"], ascending=False)
    lb.to_csv(out / "leaderboard.csv", index=False)
    (out / "status.json").write_text(json.dumps({"ok": True, "rows": int(len(lb))}, indent=2), encoding="utf-8")
    return {"ok": True, "rows": int(len(lb))}
