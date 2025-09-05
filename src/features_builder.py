from __future__ import annotations
import os, pandas as pd, numpy as np

DL_DIR = "datalake"

def _z(x): 
    s = pd.Series(x); return (s - s.rolling(20).mean()) / (s.rolling(20).std() + 1e-9)

def build_hourly_features() -> str:
    p = os.path.join(DL_DIR,"hourly_equity.parquet")
    if not os.path.exists(p): return "no_hourly"
    df = pd.read_parquet(p)
    if df.empty: return "empty"
    df = df.rename(columns={"Date":"Datetime"})
    df = df.sort_values(["Symbol","Datetime"]).reset_index(drop=True)

    def per_symbol(g):
        g = g.copy()
        g["ret_1"]  = g["Close"].pct_change()
        g["ret_5"]  = g["Close"].pct_change(5)
        g["ret_20"] = g["Close"].pct_change(20)
        g["ema12"]  = g["Close"].ewm(span=12, adjust=False).mean()
        g["ema26"]  = g["Close"].ewm(span=26, adjust=False).mean()
        g["ema_diff"] = (g["ema12"] - g["ema26"]) / (g["Close"] + 1e-9)
        g["rng"] = (g["High"] - g["Low"]) / (g["Close"] + 1e-9)
        g["vol_z20"] = _z(g["Volume"])
        g["gap_pct"] = (g["Open"] - g["Close"].shift(1)) / (g["Close"].shift(1) + 1e-9)
        return g[["Symbol","Datetime","Close","ret_1","ret_5","ret_20","ema_diff","rng","vol_z20","gap_pct"]]

    feat = df.groupby("Symbol", group_keys=False).apply(per_symbol)
    feat = feat.dropna().reset_index(drop=True)

    # VIX & GIFT
    try:
        vix = pd.read_parquet(os.path.join(DL_DIR,"vix_daily.parquet"))
        vix = vix.rename(columns={"Date":"Datetime"})[["Datetime","Close"]].rename(columns={"Close":"vix_close"})
        vix["Datetime"] = pd.to_datetime(vix["Datetime"], utc=True)
        feat = feat.merge(vix, on="Datetime", how="left")
        feat["vix_norm"] = _z(feat["vix_close"]).fillna(0.0)
    except Exception:
        feat["vix_norm"] = 0.0

    try:
        gift = pd.read_parquet(os.path.join(DL_DIR,"gift_hourly.parquet"))
        gift = gift.rename(columns={"Date":"Datetime"})[["Datetime","Close"]].rename(columns={"Close":"gift_close"})
        gift["Datetime"] = pd.to_datetime(gift["Datetime"], utc=True)
        feat = feat.merge(gift, on="Datetime", how="left")
        feat["gift_norm"] = _z(feat["gift_close"]).fillna(0.0)
    except Exception:
        feat["gift_norm"] = 0.0

    # news sentiment (placeholder)
    if "news_sent_1h" not in feat.columns:
        feat["news_sent_1h"] = 0.0

    outp = os.path.join(DL_DIR,"features_hourly.parquet")
    os.makedirs(DL_DIR, exist_ok=True)
    feat.to_parquet(outp, index=False)
    return outp
