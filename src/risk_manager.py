# src/risk_manager.py
from __future__ import annotations
import pandas as pd
from config import CONFIG

def apply_guardrails(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enforce exposure caps, numeric hygiene, and minimum trade size.
    - assumes columns: Symbol, Entry, Target, SL, proba, size_pct
    """
    if df is None or df.empty:
        return df

    d = df.copy()
    # Ensure numeric
    for c in ("Entry","Target","SL","proba","size_pct"):
        if c in d.columns:
            d[c] = pd.to_numeric(d[c], errors="coerce")

    # remove obviously bad rows
    d = d.dropna(subset=["Symbol","Entry","Target","SL","proba"])
    d = d[d["Entry"] > 0]

    # Cap by exposure
    cap = float(CONFIG.get("modes",{}).get("exposure_cap_overall", 1.0))
    if "size_pct" not in d.columns or d["size_pct"].isna().all():
        # even sizing fallback
        n = max(1, len(d))
        d["size_pct"] = round(1.0 / n, 4)
    tot = d["size_pct"].sum()
    if tot > 0:
        d["size_pct"] = (d["size_pct"] / tot) * min(1.0, cap)

    # Avoid tiny dust trades
    d = d[d["size_pct"] >= 0.02]  # >= 2% of capital

    # Clip absurd TP/SL
    d["Target"] = d[["Target","Entry"]].max(axis=1)
    d["SL"] = d[["SL","Entry"]].min(axis=1)

    # Keep only sensible columns
    keep = [c for c in ["Symbol","Entry","Target","SL","proba","size_pct","Reason","mode"] if c in d.columns]
    return d[keep].reset_index(drop=True)
