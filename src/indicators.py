from __future__ import annotations
import pandas as pd

def ema(series: pd.Series, span: int = 20) -> pd.Series:
    """
    Exponential Moving Average (EMA).
    Safe fallback → rolling mean.
    """
    try:
        return series.ewm(span=span, adjust=False).mean()
    except Exception:
        return series.rolling(window=span, min_periods=1).mean()

def add_gap_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds: prev_close, gap_abs, gap_pct, gap_dir, gap_fill_target, gap_reason
    Expects at least: Date, Symbol, Open, Close
    """
    if df is None or df.empty:
        return df
    out = df.sort_values(["Symbol", "Date"]).copy()
    out["prev_close"] = out.groupby("Symbol")["Close"].shift(1)
    out["gap_abs"] = out["Open"] - out["prev_close"]
    out["gap_pct"] = out["gap_abs"] / out["prev_close"]
    out["gap_dir"] = out["gap_abs"].apply(
        lambda x: "up" if (pd.notna(x) and x > 0) else ("down" if (pd.notna(x) and x < 0) else "flat")
    )
    out["gap_fill_target"] = out["prev_close"]
    out["gap_reason"] = out.apply(
        lambda r: f"gap_{r.gap_dir} {round(100.0*float(r.gap_pct or 0), 2)}%",
        axis=1
    )
    return out
