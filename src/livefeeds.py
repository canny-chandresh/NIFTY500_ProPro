from __future__ import annotations
import os, datetime as dt
from typing import List, Dict
import pandas as pd

try:
    import yfinance as yf
except Exception:
    yf = None

DL = "datalake"
PER = os.path.join(DL, "per_symbol")

# ---------- helpers ----------
def _fix_symbol_token(token: str) -> str:
    t = token.strip()
    if t.endswith("_NS"):  # bootstrap style (e.g., AXISBANK_NS)
        return t.replace("_NS", ".NS")
    if t.endswith("_BO"):
        return t.replace("_BO", ".BO")
    return t

def _load_symbol_universe() -> List[str]:
    """Infer universe from per_symbol filenames and sector_map; map *_NS → *.NS."""
    syms = set()
    if os.path.isdir(PER):
        for f in os.listdir(PER):
            if f.lower().endswith(".csv"):
                base = os.path.splitext(f)[0]
                syms.add(_fix_symbol_token(base))
    smap = os.path.join(DL, "sector_map.csv")
    if os.path.exists(smap):
        try:
            df = pd.read_csv(smap)
            if "Symbol" in df.columns:
                syms.update(df["Symbol"].astype(str).map(_fix_symbol_token).tolist())
        except Exception:
            pass
    if not syms:
        syms.update(["RELIANCE.NS","TCS.NS","INFY.NS","^NSEI","^NSEBANK"])
    # basic filter: keep those that at least *look* like Yahoo tickers
    out = [s for s in syms if s and isinstance(s, str)]
    return sorted(set(out))

# ---------- equities ----------
def refresh_equity_data(days: int = 60, interval: str = "1d") -> Dict:
    os.makedirs(DL, exist_ok=True)
    if yf is None:
        return {"equities_source":"none", "rows":0, "symbols":0}

    syms = _load_symbol_universe()
    if not syms:
        return {"equities_source":"none", "rows":0, "symbols":0}

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
        df["Symbol"] = syms[0]
        df = df[["Date","Symbol","Open","High","Low","Close","Volume"]]
        frames.append(df)

    if not frames:
        return {"equities_source":"yfinance", "rows":0, "symbols":0}

    out = pd.concat(frames, ignore_index=True)
    out["Date"] = pd.to_datetime(out["Date"])
    out.to_parquet(os.path.join(DL, "daily_equity.parquet"), index=False)
    out.to_csv(os.path.join(DL, "daily_equity.csv"), index=False)

    os.makedirs(PER, exist_ok=True)
    for sym in out["Symbol"].dropna().astype(str).str.upper().unique()[:20]:
        try:
            head = out[out["Symbol"] == sym].tail(60)
            head.to_csv(os.path.join(PER, f"{sym}.csv"), index=False)
        except Exception:
            pass

    return {"equities_source":"yfinance", "rows": int(len(out)), "symbols": int(out['Symbol'].nunique())}

# ---------- india vix ----------
def refresh_india_vix(days: int = 60) -> Dict:
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

# ---------- gift nifty ----------
def refresh_gift_nifty(tickers: List[str], days: int = 5) -> Dict:
    """
    Try multiple Yahoo tickers for GIFT Nifty; save datalake/gift_nifty.csv
    Fallback: leave file absent (regime will ignore).
    """
    os.makedirs(DL, exist_ok=True)
    if yf is None or not tickers:
        return {"gift_source":"none","rows":0,"ticker":None}
    for tk in tickers:
        try:
            t = yf.Ticker(tk)
            hist = t.history(period=f"{max(1, days)}d", interval="1d")
            if hist is not None and not hist.empty:
                df = hist.reset_index()[["Date","Open","High","Low","Close","Volume"]]
                df["Ticker"] = tk
                df.to_csv(os.path.join(DL, "gift_nifty.csv"), index=False)
                return {"gift_source":"yfinance","rows":int(len(df)),"ticker":tk}
        except Exception:
            continue
    return {"gift_source":"none","rows":0,"ticker":None}
