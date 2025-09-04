from __future__ import annotations
import os, pandas as pd

def _load_vix():
    p = "datalake/indiavix.csv"
    if os.path.exists(p):
        try:
            df = pd.read_csv(p)
            df["Date"] = pd.to_datetime(df["Date"])
            df = df.sort_values("Date")
            return df
        except Exception:
            return None
    return None

def apply_regime_adjustments() -> dict:
    """
    Minimal regime using India VIX.
    Extend with your NIFTY50 breadth logic if needed.
    """
    base = "NEUTRAL"
    reasons = []

    vdf = _load_vix()
    if vdf is not None and not vdf.empty:
        vix = float(vdf["VIX"].iloc[-1])
        if vix >= 20.0:
            tag = "VIX_HIGH"
            base = "BEAR" if base != "BULL" else "NEUTRAL"
        elif vix <= 13.0:
            tag = "VIX_LOW"
            base = "BULL" if base != "BEAR" else "NEUTRAL"
        else:
            tag = "VIX_MED"
        reasons.append(f"{tag}={vix:.2f}")
    else:
        reasons.append("VIX_UNAVAILABLE")

    return {"regime": base, "reason": "; ".join(reasons)}
