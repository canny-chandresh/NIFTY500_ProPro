from __future__ import annotations
import pandas as pd
from indicators import ema, add_gap_features

DL_EQ_CSV = "datalake/daily_equity.csv"

def _safe_read_csv(path: str) -> pd.DataFrame:
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()

def _last_k(df: pd.DataFrame, k: int = 60) -> pd.DataFrame:
    if df.empty: return df
    return df.sort_values("Date").groupby("Symbol").tail(k).reset_index(drop=True)

def _prepare_equity_frame() -> pd.DataFrame:
    df = _safe_read_csv(DL_EQ_CSV)
    if df.empty: return df
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"])
    for c in ["Open","High","Low","Close","Volume"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["Symbol","Date","Open","Close"])
    return _last_k(df, 60)

def _signal_block(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    df = df.sort_values(["Symbol","Date"]).copy()
    df["ema20"] = df.groupby("Symbol")["Close"].transform(lambda s: ema(s, 20))
    df["ema50"] = df.groupby("Symbol")["Close"].transform(lambda s: ema(s, 50))
    df["mom5"]  = df.groupby("Symbol")["Close"].transform(lambda s: s.pct_change(5))
    df["cross"] = (df["ema20"] > df["ema50"]).astype(float)
    return df

def _proba_from_signals(df_last: pd.DataFrame) -> pd.DataFrame:
    if df_last.empty: return df_last
    x = df_last.copy()
    x["z_mom"] = (x["mom5"] - x["mom5"].median()) / (x["mom5"].mad() or 1e-6)
    import numpy as np
    raw = 0.6 * x["cross"] + 0.4 * (1/(1+np.exp(-x["z_mom"].clip(-4,4))))
    x["proba"] = raw.clip(0.01, 0.99)
    return x

def _apply_regime_tilts(df: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    reason_tag = "NA"
    try:
        from regime import apply_regime_adjustments
        r = apply_regime_adjustments()
        reason_tag = f"{r.get('regime')} [{r.get('reason')}]"
        tag = r.get("reason","")
        m = 1.0
        if "VIX_HIGH" in tag:      m *= 0.95
        if "VIX_LOW"  in tag:      m *= 1.02
        if "NEWS_RISK_HIGH" in tag:m *= 0.97
        if "GIFT_UP" in tag:       m *= 1.01
        if "GIFT_DOWN" in tag:     m *= 0.99
        df["proba"] = (df["proba"] * m).clip(0.01, 0.995)
    except Exception:
        pass
    return df, reason_tag

def _price_to_risk_levels(entry: float) -> tuple[float,float]:
    sl = entry * 0.98
    tgt = entry * 1.02
    return (round(sl,2), round(tgt,2))

def choose_and_predict_full(top_k: int = 30) -> tuple[pd.DataFrame, str]:
    df = _prepare_equity_frame()
    if df.empty:
        return pd.DataFrame(columns=["Symbol","Entry","SL","Target","proba","Reason"]), "light"

    df = _signal_block(df)
    df = add_gap_features(df)

    last = df.sort_values("Date").groupby("Symbol").tail(1).reset_index(drop=True)
    last = _proba_from_signals(last)

    last["Reason"] = "ema20>ema50 | mom5 | " + last["gap_reason"].fillna("").astype(str)

    last["Entry"] = last["Close"].astype(float)
    sl, tgt = [], []
    for e in last["Entry"]:
        s, t = _price_to_risk_levels(float(e))
        sl.append(s); tgt.append(t)
    last["SL"] = sl; last["Target"] = tgt

    last, rtag = _apply_regime_tilts(last)
    last["Reason"] = last["Reason"] + f" | Regime: {rtag}"

    preds = last.sort_values("proba", ascending=False).head(top_k).reset_index(drop=True)
    return preds[["Symbol","Entry","SL","Target","proba","Reason"]], "light"

def train_incremental_equity():   return True
def train_incremental_intraday(): return True
def train_incremental_swing():    return True
def train_incremental_long():     return True
