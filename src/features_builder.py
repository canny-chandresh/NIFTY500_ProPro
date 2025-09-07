# src/features_builder.py
# Robust feature builder with engine-ready flags

from __future__ import annotations
from pathlib import Path
import datetime as dt
import json
import numpy as np
import pandas as pd

DL = Path("datalake")
PER = DL / "per_symbol"
OUT_DIR = DL / "features"
META_DIR = DL / "features_meta"
OUT_DIR.mkdir(parents=True, exist_ok=True)
META_DIR.mkdir(parents=True, exist_ok=True)

# ---------- basic indicators ----------
def pct_change(s, n): return s.pct_change(n)
def atr(h, l, c, n=14):
    tr = np.maximum.reduce([(h - l).to_numpy(),
                            (h - c.shift()).abs().to_numpy(),
                            (l - c.shift()).abs().to_numpy()])
    return pd.Series(tr, index=c.index).rolling(n).mean()
def ema(s, n): return s.ewm(span=n, adjust=False).mean()

# ---------- graph features ----------
def _merge_graph_features(df, symbol):
    p = DL / "features" / "graph_features_weekly.csv"
    if not p.exists():
        df["GRAPH_deg"] = np.nan
        df["GRAPH_btw"] = np.nan
        return df
    try:
        gf = pd.read_csv(p)
        row = gf[gf["symbol"] == symbol]
        if row.empty:
            df["GRAPH_deg"], df["GRAPH_btw"] = np.nan, np.nan
        else:
            df["GRAPH_deg"] = float(row["graph_deg"].iloc[0])
            df["GRAPH_btw"] = float(row["graph_btw"].iloc[0])
    except Exception:
        df["GRAPH_deg"], df["GRAPH_btw"] = np.nan, np.nan
    return df

# ---------- sources ----------
def _annotate_sources(df):
    df["live_source_equity"] = "yfinance"
    df["live_source_options"] = "nse"
    df["is_synth_options"] = 0
    df["asof_ts"] = pd.Timestamp.utcnow()
    df["data_age_min"] = 0.0
    return df

# ---------- masks ----------
def _add_masks(df):
    for col in list(df.columns):
        if col.endswith("_is_missing"): continue
        if col in ["Date","symbol","freq","asof_ts","regime_flag",
                   "live_source_equity","live_source_options","is_synth_options","data_age_min"]:
            continue
        df[f"{col}_is_missing"] = df[col].isna().astype(int)
    return df

# ---------- main builder ----------
def build_matrix(symbol: str, freq="1d"):
    raw = PER / f"{symbol}.csv"
    if not raw.exists(): raise FileNotFoundError(raw)
    df = pd.read_csv(raw, parse_dates=["Date"]).sort_values("Date")

    out = pd.DataFrame({"Date": df["Date"].values})
    # basic manual features
    out["MAN_ret1"] = df["Close"].pct_change(1)
    out["MAN_atr14"] = atr(df["High"], df["Low"], df["Close"], 14)/df["Close"]
    out["MAN_ema20slope"] = ema(df["Close"],20).diff(10)

    # target
    out["y_1d"] = df["Close"].shift(-1)/df["Close"] - 1

    # keys
    out["symbol"], out["freq"], out["regime_flag"] = symbol, freq, 0

    out = _merge_graph_features(out, symbol)
    out = _annotate_sources(out)
    out = _add_masks(out)

    out = out.dropna(subset=["y_1d"]).reset_index(drop=True)

    # save
    fpath = OUT_DIR / f"{symbol}_features.csv"
    out.to_csv(fpath,index=False)

    meta = {
        "symbol": symbol,
        "freq": freq,
        "when_utc": dt.datetime.utcnow().isoformat()+"Z",
        "rows": len(out),
        "cols": [c for c in out.columns if c!="Date"]
    }
    (META_DIR/f"{symbol}__{freq}.json").write_text(json.dumps(meta,indent=2))

    return {"ok":True,"rows":len(out),"path":str(fpath)}

def build_all(limit=None,freq="1d"):
    files = sorted(PER.glob("*.csv"))
    if limit: files=files[:limit]
    return {p.stem:build_matrix(p.stem,freq) for p in files}
