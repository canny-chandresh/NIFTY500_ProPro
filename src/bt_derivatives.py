# -*- coding: utf-8 -*-
"""
bt_derivatives.py
Defensive, data-light backtests for NSE futures & options using what you already store.
Reads:
  - datalake/per_symbol/<SYMBOL>.csv         (daily OHLC for underlyings)
  - datalake/options/chain_<SYMBOL>.jsonl    (latest chain snapshots; synthetic fallback allowed)

Outputs:
  - reports/backtest/derivatives/*.json      (summaries)
  - reports/backtest/derivatives/*.csv       (trade ledgers)

Strategies (toy but useful to compare engines quickly):
  Futures:
    - F1: Trend-follow daily (MA cross) long/flat with costs.
  Options (index + stocks when chain available):
    - O1: ATM straddle day-hold (vol/mean-rev diagnostic)
    - O2: Vertical call spread (buy ATM, sell +1 step) directional long
Notes:
  • These are approximation backtests for ranking/validation (NOT broker-accurate).
  • If a chain snapshot is synthetic, it's labeled in outputs; runs still proceed.
"""

from __future__ import annotations
import json, math, glob, traceback
from dataclasses import dataclass
from typing import Dict, Any, List, Tuple
from pathlib import Path
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd

# Local imports
try:
    from config import CONFIG
except Exception:
    CONFIG = {"paths": {"datalake": "datalake", "reports": "reports"}, "options": {"indices": ["NIFTY","BANKNIFTY"], "stocks": []}}

DL = Path(CONFIG["paths"]["datalake"])
RPT = Path(CONFIG["paths"]["reports"])
OUT = RPT / "backtest" / "derivatives"
OUT.mkdir(parents=True, exist_ok=True)

# ------------------------ Helpers ------------------------

def _load_underlying(sym: str, years: int = 5) -> pd.DataFrame:
    fp = DL / "per_symbol" / f"{sym}.csv"
    if not fp.exists(): return pd.DataFrame()
    try:
        df = pd.read_csv(fp, parse_dates=["date"])
        if years:
            cutoff = pd.Timestamp.utcnow().tz_localize(None) - pd.Timedelta(days=365*years)
            df = df[df["date"] >= cutoff]
        if "close" not in df and "adj_close" in df:
            df["close"] = df["adj_close"]
        return df[["date","close"]].dropna().sort_values("date")
    except Exception:
        traceback.print_exc()
        return pd.DataFrame()

def _load_chain_jsonl(sym: str, max_snaps: int = 40) -> List[dict]:
    fp = DL / "options" / f"chain_{sym}.jsonl"
    if not fp.exists(): return []
    out = []
    try:
        with fp.open("r", encoding="utf-8") as f:
            for i, line in enumerate(f):
                if not line.strip(): continue
                try:
                    out.append(json.loads(line))
                except Exception:
                    pass
                if len(out) >= max_snaps: break
    except Exception:
        traceback.print_exc()
    return out

def _to_returns(close: pd.Series) -> pd.Series:
    return close.pct_change().fillna(0.0)

def _sharpe(rets: np.ndarray, scale: float = math.sqrt(252.0)) -> float:
    if len(rets) == 0: return 0.0
    m, s = np.nanmean(rets), np.nanstd(rets, ddof=1)
    return float((m / s) * scale) if s > 0 else 0.0

def _max_dd(eq: np.ndarray) -> float:
    peak = -1e18; mdd = 0.0
    for x in eq:
        peak = max(peak, x)
        mdd = min(mdd, x - peak)
    return float(mdd)

# ------------------------ Futures: MA trend (F1) ------------------------

@dataclass
class FutCfg:
    fast: int = 10
    slow: int = 30
    cost_bps: float = 2.0      # one side; daily rebalance ~ 4 bps round-trip
    slippage_bps: float = 3.0

def backtest_futures(sym: str, years: int = 5, cfg: FutCfg = FutCfg()) -> Dict[str, Any]:
    px = _load_underlying(sym, years=years)
    if px.empty: 
        return {"symbol": sym, "trades": 0, "winrate": 0.0, "sharpe": 0.0, "max_dd": 0.0, "note": "no data"}

    df = px.copy()
    df["ma_f"] = df["close"].rolling(cfg.fast).mean()
    df["ma_s"] = df["close"].rolling(cfg.slow).mean()
    df["pos"]  = (df["ma_f"] > df["ma_s"]).astype(int)  # long/flat
    df["ret"]  = _to_returns(df["close"])
    # Transaction costs on position change
    df["chg"]  = df["pos"].diff().abs().fillna(0.0)
    roundtrip_cost = (cfg.cost_bps + cfg.slippage_bps) / 1e4
    df["pnl"]  = df["pos"].shift(1).fillna(0.0) * df["ret"] - df["chg"]*roundtrip_cost

    rets = df["pnl"].fillna(0.0).values
    wr   = float((rets > 0).mean())
    sh   = _sharpe(rets)
    mdd  = _max_dd(np.cumsum(rets))
    trades = int(df["chg"].sum())  # number of entries roughly
    ledger = df[["date","pos","ret","pnl"]].copy()
    ledger.to_csv(OUT / f"fut_{sym}_ledger.csv", index=False)

    return {"symbol": sym, "trades": trades, "winrate": wr, "sharpe": sh, "max_dd": mdd}

# ------------------------ Options: Straddle (O1) ------------------------

@dataclass
class OptCfg:
    step: int = 50              # strike granularity for indices
    cost_frac: float = 0.005    # 0.5% entry/exit per leg approximated
    hold_days: int = 1

def _latest_chain_snapshot(sym: str) -> dict:
    snaps = _load_chain_jsonl(sym, max_snaps=1)
    return snaps[-1] if snaps else {}

def _atm_price_from_chain(chain: dict) -> Tuple[float, bool]:
    """Return approx underlying and synthetic flag."""
    if not chain: return 0.0, True
    if "underlyingValue" in chain.get("records", {}):
        return float(chain["records"]["underlyingValue"]), bool(chain.get("synthetic", False))
    # synthetic style fallback used by our options_ingest
    if "underlying" in chain:
        return float(chain["underlying"]), bool(chain.get("synthetic", True))
    return 0.0, True

def _pick_atm_row(chain: dict, u: float, step: int = 50) -> Dict[str, Any]:
    """Approx choose ATM strike row from chain.records.data or synthetic strikes."""
    if "records" in chain and "data" in chain["records"]:
        rows = chain["records"]["data"]
        # Each row typically has CE/PE dicts with 'strikePrice'
        best, bdist = None, 1e9
        for r in rows:
            k = r.get("strikePrice")
            if k is None: continue
            dist = abs(float(k) - u)
            if dist < bdist:
                best, bdist = r, dist
        return best or {}
    # synthetic
    strikes = chain.get("strikes", [])
    best, bdist = None, 1e9
    for r in strikes:
        k = r.get("strike"); 
        if k is None: continue
        dist = abs(float(k) - u)
        if dist < bdist:
            best, bdist = r, dist
    return best or {}

def backtest_straddle(sym: str, years: int = 1, cfg: OptCfg = OptCfg()) -> Dict[str, Any]:
    """
    Day-hold ATM straddle using last known chain snapshot prices as proxy for legs.
    This is a *diagnostic* (not live-accurate). Uses underlying daily move to exit next day.
    """
    px = _load_underlying(sym, years=years)
    if px.empty:
        return {"symbol": sym, "trades": 0, "winrate": 0.0, "sharpe": 0.0, "max_dd": 0.0, "note": "no underlying"}
    chain = _latest_chain_snapshot(sym)
    u0, synth = _atm_price_from_chain(chain)
    atm = _pick_atm_row(chain, u0, cfg.step)
    if not atm:
        return {"symbol": sym, "trades": 0, "winrate": 0.0, "sharpe": 0.0, "max_dd": 0.0, "note": "no chain"}
    # Get CE/PE leg costs
    def _leg_price(x): 
        if isinstance(x, dict):
            for key in ("lastPrice","LTP","ltp","close","price"): 
                if key in x: 
                    try: return float(x[key])
                    except Exception: pass
        return None
    ce = _leg_price(atm.get("CE", {})) or 0.0
    pe = _leg_price(atm.get("PE", {})) or 0.0
    if ce <= 0.0 or pe <= 0.0:
        return {"symbol": sym, "trades": 0, "winrate": 0.0, "sharpe": 0.0, "max_dd": 0.0, "note": "bad legs"}
    entry_cost = ce + pe
    # Simulate rolling day-hold PnL with proxy = abs(next day return)*u0 scaled to option premium
    df = px.copy().sort_values("date")
    df["ret_u"] = df["close"].pct_change().fillna(0.0)
    # very rough mapping: payoff ~ entry_cost * (|ret_u_next| / typical_day_vol)
    typical = max(0.005, df["ret_u"].rolling(60).std().median())
    df["payoff"] = entry_cost * (df["ret_u"].abs() / typical)
    # costs (2 legs entry/exit)
    cost = 2 * cfg.cost_frac * entry_cost
    df["pnl"] = df["payoff"] - cost
    rets = df["pnl"].iloc[1:].values  # ignore first NaN-ish
    wr, sh, mdd = float((rets > 0).mean()), _sharpe(rets), _max_dd(np.cumsum(rets))
    ledger = df[["date","ret_u","pnl"]]
    ledger.to_csv(OUT / f"opt_straddle_{sym}.csv", index=False)
    return {"symbol": sym, "trades": int(len(rets)), "winrate": wr, "sharpe": sh, "max_dd": mdd, "synthetic": synth}

# ------------------------ Options: Vertical Call Spread (O2) ------------------------

@dataclass
class VertCfg:
    step: int = 50
    cost_frac: float = 0.004
    hold_days: int = 3

def backtest_vertical_call(sym: str, years: int = 1, cfg: VertCfg = VertCfg()) -> Dict[str, Any]:
    """
    Directional long: buy ATM call, sell +1 step call. Exit after hold_days or if underlying falls -x%.
    Uses underlying drift proxy for payoff; for ranking only.
    """
    px = _load_underlying(sym, years=years)
    if px.empty: 
        return {"symbol": sym, "trades": 0, "winrate": 0.0, "sharpe": 0.0, "max_dd": 0.0, "note": "no underlying"}
    chain = _latest_chain_snapshot(sym)
    u0, synth = _atm_price_from_chain(chain)
    atm = _pick_atm_row(chain, u0, cfg.step)
    if not atm: 
        return {"symbol": sym, "trades": 0, "winrate": 0.0, "sharpe": 0.0, "max_dd": 0.0, "note": "no chain"}

    def _leg_price(x): 
        if isinstance(x, dict):
            for key in ("lastPrice","LTP","ltp","close","price"): 
                if key in x: 
                    try: return float(x[key])
                    except Exception: pass
        return None
    c_buy = _leg_price(atm.get("CE", {}))
    if not c_buy or c_buy <= 0: 
        return {"symbol": sym, "trades": 0, "winrate": 0.0, "sharpe": 0.0, "max_dd": 0.0, "note": "bad CE"}
    # sold call (ATM + step) — use 70% of buy as placeholder if not found
    c_sell = 0.7 * c_buy

    df = px.copy().sort_values("date")
    df["ret_u"] = df["close"].pct_change().fillna(0.0)
    # naive payoff proxy over window
    win = cfg.hold_days
    roll = df["ret_u"].rolling(win).sum().shift(-win+1)  # forward-looking percent move proxy
    payoff = (roll * u0).clip(lower=-c_buy)  # capped loss at premium paid
    cost = cfg.cost_frac * (c_buy + c_sell) * 2
    pnl = payoff - cost
    pnl = pnl.dropna()
    rets = pnl.values
    wr, sh, mdd = float((rets > 0).mean()), _sharpe(rets), _max_dd(np.cumsum(rets))
    ledger = pd.DataFrame({"date": df["date"].iloc[:len(rets)].values, "pnl": rets})
    ledger.to_csv(OUT / f"opt_vertical_{sym}.csv", index=False)
    return {"symbol": sym, "trades": int(len(rets)), "winrate": wr, "sharpe": sh, "max_dd": mdd, "synthetic": synth}

# ------------------------ Master: run_all ------------------------

def run_all(years_fut: int = 5, years_opt: int = 1) -> Dict[str, Any]:
    uni = CONFIG.get("universe", [])
    indices = CONFIG.get("options", {}).get("indices", [])
    stocks  = CONFIG.get("options", {}).get("stocks", [])
    res = {"futures": [], "straddle": [], "vertical": []}

    # Futures on universe names (use your big liquid names)
    for s in uni:
        sym = s.replace(".NS","")
        try:
            res["futures"].append(backtest_futures(sym, years=years_fut))
        except Exception:
            traceback.print_exc()

    # Options on indices + selected stocks
    for s in (indices + stocks):
        sym = s.replace(".NS","")
        try:
            res["straddle"].append(backtest_straddle(sym, years=years_opt))
        except Exception:
            traceback.print_exc()
        try:
            res["vertical"].append(backtest_vertical_call(sym, years=years_opt))
        except Exception:
            traceback.print_exc()

    # Write summaries
    (OUT / "summary_futures.json").write_text(json.dumps(res["futures"], indent=2))
    (OUT / "summary_straddle.json").write_text(json.dumps(res["straddle"], indent=2))
    (OUT / "summary_vertical.json").write_text(json.dumps(res["vertical"], indent=2))

    # Compact aggregates
    def _agg(items: List[Dict[str,Any]]) -> Dict[str, float]:
        if not items: return {"trades": 0, "winrate": 0.0, "sharpe": 0.0}
        wr = [x.get("winrate",0.0) for x in items]; sh = [x.get("sharpe",0.0) for x in items]
        tr = [x.get("trades",0)   for x in items]
        w  = [max(t,1) for t in tr]
        return {
            "trades": int(sum(tr)),
            "winrate": float(np.average(wr, weights=w)),
            "sharpe":  float(np.average(sh, weights=w))
        }

    agg = {
        "futures":  _agg(res["futures"]),
        "straddle": _agg(res["straddle"]),
        "vertical": _agg(res["vertical"]),
    }
    (OUT / "aggregate.json").write_text(json.dumps(agg, indent=2))
    print(json.dumps(agg, indent=2))
    return {"details": res, "aggregate": agg}
