# src/regime.py
import os, pandas as pd
from config import CONFIG, DL
from indicators import ema, pct_above_sma

def _load_daily():
    fp = DL("daily_equity")
    if os.path.exists(fp): return pd.read_parquet(fp)
    fp = DL("daily_equity_csv")
    if os.path.exists(fp): return pd.read_csv(fp, parse_dates=["Date"])
    return pd.DataFrame()

def classify_regime():
    if not CONFIG["features"]["regime_v1"]:
        return {"regime":"neutral","reason":"feature disabled","metrics":{}}

    df = _load_daily()
    if df.empty:
        return {"regime":"neutral","reason":"no data","metrics":{}}

    idx = df[df["Symbol"].isin(["NIFTY50","^NSEI","NIFTY.NS"])].copy()
    if idx.empty:
        idx = df[df["Symbol"]=="RELIANCE.NS"].copy()  # fallback proxy

    idx = idx.sort_values("Date")
    short = CONFIG["regime"]["ema_short"]
    long  = CONFIG["regime"]["ema_long"]
    breadth_ma = CONFIG["regime"]["breadth_ma"]

    idx["ema_s"] = ema(idx["Close"], short)
    idx["ema_l"] = ema(idx["Close"], long)
    idx["bullish"] = (idx["ema_s"] > idx["ema_l"]).astype(int)

    last_day = idx["Date"].max()
    uni = df[df["Date"]<=last_day]

    def _sym_breadth(g):
        g = g.sort_values("Date")
        pa = pct_above_sma(g["Close"], breadth_ma)
        return pd.Series({"breadth": pa.iloc[-1] if len(pa)>0 else 0})
    b = uni.groupby("Symbol").apply(_sym_breadth).reset_index(drop=True)
    breadth = b["breadth"].mean() if not b.empty else 0.5

    bull_thr = CONFIG["regime"]["bull_breadth_min"]
    bear_thr = CONFIG["regime"]["bear_breadth_max"]
    trend_up = int(idx.iloc[-1]["bullish"])

    if trend_up and breadth >= bull_thr:
        regime = "bull"; reason = f"EMA{short}>{long} & breadth {breadth:.2f}"
    elif (not trend_up) and breadth <= bear_thr:
        regime = "bear"; reason = f"EMA{short}<{long} & breadth {breadth:.2f}"
    else:
        regime = "neutral"; reason = f"trend {trend_up}, breadth {breadth:.2f}"

    return {"regime": regime, "reason": reason, "metrics": {"breadth": breadth}}

def apply_regime_adjustments():
    return classify_regime()
