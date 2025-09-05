from __future__ import annotations
import os, datetime as dt
from typing import List, Dict, Any
import pandas as pd

try:
    import yfinance as yf
except Exception:
    yf = None

from config import CONFIG

DL_DIR = "datalake"

# ---------- basics ----------
def _ensure_dir(): os.makedirs(DL_DIR, exist_ok=True)
def _write_parquet(df: pd.DataFrame, path: str):
    if df is None or df.empty: return
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    try: df.to_parquet(path, index=False)
    except Exception: df.to_csv(path.replace(".parquet",".csv"), index=False)

def _fix_symbol(s: str) -> str:
    s = (s or "").strip().upper()
    if not s: return s
    if not s.endswith(".NS"): s += ".NS"
    return s

def _load_universe() -> List[str]:
    p = CONFIG["data"].get("symbols_file","")
    n = int(CONFIG["data"].get("default_universe",300))
    if p and os.path.exists(p):
        df = pd.read_csv(p)
        col = "Symbol" if "Symbol" in df.columns else df.columns[0]
        syms = df[col].dropna().astype(str).tolist()
    else:
        syms = ["RELIANCE","TCS","HDFCBANK","INFY","ICICIBANK","SBIN","ITC","BHARTIARTL","LT","HINDUNILVR"]
    syms = [_fix_symbol(s) for s in syms][:n]
    return syms

# ---------- Yahoo wrapper ----------
def _yf_download(symbols: List[str], period: str, interval: str) -> pd.DataFrame:
    if yf is None: return pd.DataFrame()
    data = yf.download(
        tickers=" ".join(symbols),
        period=period, interval=interval, group_by="ticker",
        auto_adjust=False, threads=True, progress=False
    )
    frames = []
    for s in symbols:
        try: d = data[s]
        except Exception: continue
        d = d.reset_index().rename(columns={"Datetime":"Date"})
        d["Symbol"] = s
        frames.append(d)
    if not frames: return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    m = {"Open":"Open","High":"High","Low":"Low","Close":"Close","Adj Close":"AdjClose","Volume":"Volume"}
    for k,v in m.items():
        if k in df.columns: df[v] = df[k]
    if "AdjClose" not in df.columns and "Close" in df.columns:
        df["AdjClose"] = df["Close"]
    df["Date"] = pd.to_datetime(df["Date"], utc=True)
    df["Source"] = f"yfinance:{interval}"
    return df[["Symbol","Date","Open","High","Low","Close","AdjClose","Volume","Source"]].dropna()

# ---------- public helpers ----------
def refresh_equity_daily(days: int = None) -> Dict[str,Any]:
    _ensure_dir()
    days = days or CONFIG["data"]["fetch"]["daily_days"]
    syms = _load_universe()
    df = _yf_download(syms, period=f"{days}d", interval="1d")
    _write_parquet(df, os.path.join(DL_DIR,"daily_equity.parquet"))
    return {"equities_source":"yfinance","interval":"1d","rows":len(df),"symbols":len(df["Symbol"].unique()) if not df.empty else 0}

def refresh_equity_hourly() -> Dict[str,Any]:
    _ensure_dir()
    syms = _load_universe()
    df = _yf_download(syms, period=f"{CONFIG['data']['fetch']['hourly_days']}d", interval="60m")
    _write_parquet(df, os.path.join(DL_DIR,"hourly_equity.parquet"))
    return {"equities_source":"yfinance","interval":"60m","rows":len(df),"symbols":len(df["Symbol"].unique()) if not df.empty else 0}

def refresh_equity_minute() -> Dict[str,Any]:
    _ensure_dir()
    syms = _load_universe()
    df = _yf_download(syms, period=f"{CONFIG['data']['fetch']['minute_days']}d", interval="1m")
    _write_parquet(df, os.path.join(DL_DIR,"minute_equity.parquet"))
    # Optional roll-up minute→hourly
    if not df.empty:
        d = df.copy()
        d["Date"] = pd.to_datetime(d["Date"], utc=True)
        d["Hour"] = d["Date"].dt.floor("60min")
        agg = d.groupby(["Symbol","Hour"]).agg(
            Open=("Open","first"), High=("High","max"), Low=("Low","min"),
            Close=("Close","last"), AdjClose=("AdjClose","last"), Volume=("Volume","sum"),
            Source=("Source","last")
        ).reset_index().rename(columns={"Hour":"Date"})
        agg["Source"] = "rollup:1m->60m"
        p_hour = os.path.join(DL_DIR,"hourly_equity.parquet")
        old = pd.read_parquet(p_hour) if os.path.exists(p_hour) else pd.DataFrame()
        hourly = pd.concat([old, agg], ignore_index=True)
        hourly = hourly.drop_duplicates(subset=["Symbol","Date"]).sort_values(["Symbol","Date"])
        _write_parquet(hourly, p_hour)
    return {"equities_source":"yfinance","interval":"1m","rows":len(df),"symbols":len(df["Symbol"].unique()) if not df.empty else 0}

def refresh_india_vix(days: int = 30) -> Dict[str,Any]:
    df = _yf_download(["^INDIAVIX"], period=f"{days}d", interval="1d")
    if not df.empty:
        df["Symbol"]="INDIAVIX"; _write_parquet(df, os.path.join(DL_DIR,"vix_daily.parquet"))
    return {"vix_source":"yfinance","rows":len(df)}

def refresh_gift_nifty(tickers: List[str], days: int = 10) -> Dict[str,Any]:
    if not tickers: tickers = ["^NSEI"]
    df = pd.DataFrame()
    for t in tickers:
        x = _yf_download([t], period=f"{days}d", interval="60m")
        if not x.empty:
            x["Symbol"]="GIFTNIFTY"; df=x; break
    if not df.empty:
        _write_parquet(df, os.path.join(DL_DIR,"gift_hourly.parquet"))
    return {"gift_source":"yfinance","rows":len(df)}
