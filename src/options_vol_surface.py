# src/options_vol_surface.py
from __future__ import annotations
import numpy as np, pandas as pd
from pathlib import Path

def fit_vol_surface(chain_df: pd.DataFrame, asof_utc: str | None = None):
    """
    Fit a simple quadratic vol 'surface' using only rows with fetched_utc <= asof_utc.
    If asof_utc is None, uses current UTC (safe).
    """
    if asof_utc is None:
        asof = pd.Timestamp.utcnow()
    else:
        asof = pd.Timestamp(asof_utc)

    df = chain_df.copy()
    if "fetched_utc" in df.columns:
        df = df[pd.to_datetime(df["fetched_utc"], errors="coerce") <= asof]
    df = df.dropna(subset=["strike","iv"])
    if df.empty:
        return {"ok": False, "reason": "no_data"}

    coeffs = np.polyfit(df["strike"].astype(float), df["iv"].astype(float), 2)
    meta = {"asof_utc": asof.isoformat()+"Z", "rows": int(len(df))}
    return {"ok": True, "coeffs": coeffs.tolist(), "meta": meta}

def implied_vol(coeffs, strike: float) -> float:
    return float(np.polyval(np.array(coeffs, dtype=float), float(strike)))
