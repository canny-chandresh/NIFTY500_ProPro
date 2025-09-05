from __future__ import annotations
import os, json, datetime as dt
import pandas as pd

RPT_DIR = "reports/metrics"
os.makedirs(RPT_DIR, exist_ok=True)

def _load_csv(path: str) -> pd.DataFrame:
    try:
        if os.path.exists(path):
            return pd.read_csv(path)
    except Exception:
        pass
    return pd.DataFrame()

def _parse_time(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty: return df
    for c in ("Timestamp","Date","Datetime","time","created_at"):
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce", utc=True)
            break
    return df

def _get_cost_bps():
    # Costs from CONFIG if available; else sensible defaults
    try:
        from config import CONFIG
        c = (CONFIG or {}).get("paper_costs", {})
        return float(c.get("equity_bps", 3.0)), float(c.get("options_bps", 30.0)), float(c.get("futures_bps", 5.0))
    except Exception:
        return 3.0, 30.0, 5.0

def _stats_from_trades(df: pd.DataFrame, entry="Entry", tgt="Target", sl="SL", kind="equity") -> dict:
    """
    Compute trades, win_rate, pnl (currency units), ret_vol, max_drawdown.
    If 'PnL' column exists, we still recompute proxy and prefer 'PnL' only if present & numeric.
    Costs are subtracted via simple bps model.
    """
    if df is None or df.empty:
        return {"trades": 0, "win_rate": 0.0, "pnl": 0.0, "ret_vol": 0.0, "max_drawdown": 0.0}

    eq_bps, opt_bps, fut_bps = _get_cost_bps()
    cost_bps = {"equity":eq_bps, "options":opt_bps, "futures":fut_bps}.get(kind, eq_bps) / 10000.0

    d = df.copy()
    for col in (entry, tgt, sl):
        if col not in d.columns:
            d[col] = 0.0
    d[entry] = pd.to_numeric(d[entry], errors="coerce").fillna(0.0)
    d[tgt]   = pd.to_numeric(d[tgt],   errors="coerce").fillna(d[entry])
    d[sl]    = pd.to_numeric(d[sl],    errors="coerce").fillna(d[entry])

    # Proxy realized return per trade: (win leg + loss leg) / entry
    per_trade_ret = ((d[tgt] - d[entry]).clip(lower=0) + (d[sl] - d[entry]).clip(upper=0)) / d[entry].replace(0, 1)
    # Subtract simple round-trip costs
    per_trade_ret = per_trade_ret - 2.0 * cost_bps

    pnl_proxy = (per_trade_ret * d[entry]).sum()  # in "entry currency" units
    wins = (d[tgt] > d[entry]).sum()
    trades = max(1, len(d))
    wr = wins / trades
    vol = float(per_trade_ret.std()) if len(per_trade_ret) > 1 else 0.0

    # Max drawdown on cumulative equity curve
    eq = (1.0 + per_trade_ret.fillna(0)).cumprod()
    peak = 1.0
    max_dd = 0.0
    for v in eq:
        peak = max(peak, v)
        max_dd = min(max_dd, (v / peak - 1.0))
    max_dd = abs(float(max_dd))

    return {
        "trades": int(len(d)),
        "win_rate": float(wr),
        "pnl": float(pnl_proxy),
        "ret_vol": float(abs(vol)),
        "max_drawdown": float(max_dd)
    }

def summarize_last_n(days: int = 10) -> dict:
    auto = _parse_time(_load_csv("datalake/paper_trades.csv"))
    algo = _parse_time(_load_csv("datalake/algo_paper.csv"))
    opts = _parse_time(_load_csv("datalake/options_paper.csv"))
    futs = _parse_time(_load_csv("datalake/futures_paper.csv"))

    since = dt.datetime.utcnow() - dt.timedelta(days=days)
    def _slice(df):
        if df is None or df.empty: return df
        time_col = next((c for c in ("Timestamp","Date","Datetime","time","created_at") if c in df.columns), None)
        return df[df[time_col] >= since].copy() if time_col else df

    autoN = _slice(auto); algoN = _slice(algo); optsN = _slice(opts); futsN = _slice(futs)

    out = {
        "when_utc": dt.datetime.utcnow().isoformat()+"Z",
        "window_days": days,
        "AUTO":    _stats_from_trades(autoN, kind="equity"),
        "ALGO":    _stats_from_trades(algoN, kind="equity"),
        "OPTIONS": _stats_from_trades(optsN, entry="EntryPrice", tgt="Target", sl="SL", kind="options"),
        "FUTURES": _stats_from_trades(futsN, entry="EntryPrice", tgt="Target", sl="SL", kind="futures"),
    }
    with open(os.path.join(RPT_DIR, "rolling_metrics.json"), "w") as f:
        json.dump(out, f, indent=2)
    return out
