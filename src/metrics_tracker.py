# src/metrics_tracker.py
from __future__ import annotations
import os, json, datetime as dt
import pandas as pd

DL = "datalake"
MET_DIR = "reports/metrics"

def _ensure_dirs():
    os.makedirs(MET_DIR, exist_ok=True)

def _now_utc():
    return dt.datetime.utcnow().replace(microsecond=0)

def _read_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path): return pd.DataFrame()
    try: return pd.read_csv(path)
    except Exception: return pd.DataFrame()

def _save_json(path: str, obj):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path,"w") as f: json.dump(obj, f, indent=2)

def summarize_last_n(days: int = 10) -> dict:
    """
    Reads paper_trades.csv and paper_fills.csv if present,
    computes simple win_rate/Sharpe-like and DD proxy for AUTO and ALGO.
    Expected columns (trades): when_utc, engine, Symbol, fill_price, Target, SL, status
    For simplicity, we assume each paper trade exits if Close hits TP/SL during the window.
    If your execution module already writes PnL rows, adapt below to read that instead.
    """
    _ensure_dirs()

    trades = _read_csv(os.path.join(DL, "paper_trades.csv"))
    fills  = _read_csv(os.path.join(DL, "paper_fills.csv"))  # optional

    # quick filter last N days by timestamp if present
    since = _now_utc() - dt.timedelta(days=days)
    def _parse_ts(x):
        try: return dt.datetime.fromisoformat(str(x).replace("Z",""))
        except Exception: return None
    if "when_utc" in trades.columns:
        trades["__ts"] = trades["when_utc"].map(_parse_ts)
        trades = trades.dropna(subset=["__ts"])
        trades = trades[trades["__ts"] >= since]

    if trades.empty:
        out = {"AUTO":{"win_rate":0.0,"sharpe":0.0,"max_drawdown":0.0},
               "ALGO":{"win_rate":0.0,"sharpe":0.0,"max_drawdown":0.0}}
        _save_json(os.path.join(MET_DIR,"rolling_metrics.json"), out)
        return out

    # very rough PnL proxy: assume immediate TP/SL resolution probability by proba
    # If you have realized exits, replace this with actual PnL aggregation.
    def _pnl_proxy(row):
        entry = float(row.get("fill_price", row.get("Entry", 0.0)) or 0.0)
        tgt   = float(row.get("Target", 0.0) or 0.0)
        sl    = float(row.get("SL", 0.0) or 0.0)
        pr    = float(row.get("proba", 0.5) or 0.5)
        # expected return approx:
        up = (tgt - entry)/max(1e-9, entry)
        dn = (entry - sl)/max(1e-9, entry)
        exp = pr*up - (1.0-pr)*dn
        return exp

    trades["exp_ret"] = trades.apply(_pnl_proxy, axis=1)
    auto = trades[trades["engine"]=="AUTO"]["exp_ret"]
    algo = trades[trades["engine"]=="ALGO"]["exp_ret"]

    def _metrics(series: pd.Series) -> dict:
        if series is None or series.empty:
            return {"win_rate":0.0,"sharpe":0.0,"max_drawdown":0.0}
        # proxy winrate as fraction of positive exp_ret
        wr = float((series > 0).mean())
        mu = float(series.mean())
        sig= float(series.std(ddof=1) if len(series)>1 else 0.0)
        sharpe = (mu / (sig+1e-9)) * (len(series)**0.5) if sig>0 else (1.0 if mu>0 else -1.0)
        # drawdown proxy: cumulative sum min vs max
        eq = series.cumsum()
        if eq.empty:
            dd = 0.0
        else:
            roll_max = eq.cummax()
            dd = float(((eq - roll_max).min()) or 0.0)
            dd = abs(dd)
        return {"win_rate": round(wr,4), "sharpe": round(sharpe,3), "max_drawdown": round(dd,4)}

    out = {"AUTO": _metrics(auto), "ALGO": _metrics(algo)}
    _save_json(os.path.join(MET_DIR,"rolling_metrics.json"), out)
    return out
