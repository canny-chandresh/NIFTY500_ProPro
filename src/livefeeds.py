from __future__ import annotations
import os, csv, json, datetime as dt
from typing import List, Dict
import pandas as pd

# Attempt to import yfinance; callers handle absence gracefully
try:
    import yfinance as yf
except Exception:
    yf = None

DL = "datalake"
PER = os.path.join(DL, "per_symbol")

def _load_symbol_universe() -> List[str]:
    """
    Try to infer symbols from:
    - datalake/per_symbol/*.csv (filenames as symbols)
    - datalake/sector_map.csv (Symbol column)
    Fall back to a small starter set if nothing found.
    """
    syms = set()
    if os.path.isdir(PER):
        for f in os.listdir(PER):
            if f.lower().endswith(".csv"):
                syms.add(os.path.splitext(f)[0])
    smap = os.path.join(DL, "sector_map.csv")
    if os.path.exists(smap):
        try:
            df = pd.read_csv(smap)
            if "Symbol" in df.columns:
                syms.update(df["Symbol"].astype(str).str.upper().tolist())
        except Exception:
            pass
    # Minimal fallbacks (you can extend)
    if not syms:
        syms.update(["RELIANCE.NS", "TCS.NS", "INFY.NS", "^NSEI", "^NSEBANK"])
    return sorted(syms)

def refresh_equity_data(days: int = 60, interval: str = "1d") -> Dict:
    """
    Pull last `days` of OHLC for inferred universe; write:
      - datalake/daily_equity.parquet + csv
      - optionally datalake/per_symbol/<sym>.csv (head)
    Returns a dict with source metadata.
    """
    os.makedirs(DL, exist_ok=True)
    syms = _load_symbol_universe()
    if yf is None:
        # no yfinance available
        return {"equities_source":"none", "rows":0, "symbols":0}

    # yfinance: use multi-download
    try:
        hist = yf.download(
            tickers=syms,
            period=f"{max(1, days)}d",
            interval=interval,
            group_by="ticker",
            auto_adjust=False,
            threads=True,
            progress=False
        )
    except Exception:
        return {"equities_source":"error", "rows":0, "symbols":0}

    # Normalize to long-form: Date, Symbol, Open, High, Low, Close, Volume
    frames = []
    if isinstance(hist.columns, pd.MultiIndex):
        for sym in hist.columns.levels[0]:
            try:
                df = hist[sym].reset_index()
                df["Symbol"] = str(sym)
                df = df.rename(columns=str).rename(columns={"Adj Close":"AdjClose"})
                df = df[["Date","Symbol","Open","High","Low","Close","Volume"]]
                frames.append(df)
            except Exception:
                continue
    else:
        df = hist.reset_index()
        df["Symbol"] = syms[0] if syms else "UNKNOWN"
        df = df[["Date","Symbol","Open","High","Low","Close","Volume"]]
        frames.append(df)

    if not frames:
        return {"equities_source":"yfinance", "rows":0, "symbols":0}

    out = pd.concat(frames, ignore_index=True)
    out["Date"] = pd.to_datetime(out["Date"])
    # Save parquet + csv
    out.to_parquet(os.path.join(DL, "daily_equity.parquet"), index=False)
    out.to_csv(os.path.join(DL, "daily_equity.csv"), index=False)

    # Optionally, refresh a few per_symbol heads for visibility
    os.makedirs(PER, exist_ok=True)
    for sym in out["Symbol"].dropna().astype(str).str.upper().unique()[:20]:
        try:
            head = out[out["Symbol"] == sym].tail(60)  # last 60 rows
            head.to_csv(os.path.join(PER, f"{sym}.csv"), index=False)
        except Exception:
            pass

    return {"equities_source":"yfinance", "rows": int(len(out)), "symbols": int(out['Symbol'].nunique())}

def refresh_india_vix(days: int = 60) -> Dict:
    """
    Pull ^INDIAVIX daily (Close) and write datalake/indiavix.csv
    """
    os.makedirs(DL, exist_ok=True)
    if yf is None:
        return {"vix_source":"none","rows":0}
    try:
        t = yf.Ticker("^INDIAVIX")
        hist = t.history(period=f"{max(1, days)}d", interval="1d")
        if hist is None or hist.empty:
            return {"vix_source":"none","rows":0}
        df = hist.reset_index()[["Date","Close"]].rename(columns={"Close":"VIX"})
        df.to_csv(os.path.join(DL, "indiavix.csv"), index=False)
        return {"vix_source":"yfinance","rows":int(len(df))}
    except Exception:
        return {"vix_source":"none","rows":0}
