from __future__ import annotations
"""
Model selector for NIFTY500 Pro Pro Screener.

Contract:
    choose_and_predict_full(top_k: int = 5) -> (pandas.DataFrame, str)

- Tries Deep Learning (DL) first if:
    * dl_runner reports "dl_ready"
    * DL kill-switch is not suspending (dl_runner handles this internally)
- Falls back to Light/Robust model (model_swing.predict_today)
- Applies sector caps if enabled in config
- Normalizes columns to: Symbol, Entry, SL, Target, proba, Reason
"""

import os
from typing import Tuple
import pandas as pd

# ---- Optional, non-fatal imports ----
try:
    from config import CONFIG
except Exception:
    CONFIG = {
        "selection": {"sector_cap_enabled": False, "max_per_sector": 2, "max_total": 5}
    }

# sector map helper (optional)
def _load_sector_map() -> pd.DataFrame:
    """
    Attempts to load sector mapping from datalake/sector_map.csv
    with columns: Symbol, Sector
    """
    try:
        p = os.path.join("datalake", "sector_map.csv")
        if os.path.exists(p):
            df = pd.read_csv(p)
            # Normalize expected columns
            if "Symbol" not in df.columns:
                # try best-effort first column
                df = df.rename(columns={df.columns[0]: "Symbol"})
            if "Sector" not in df.columns:
                # fallback: try 'Industry' or 'SectorName'
                for c in ["Industry", "SectorName"]:
                    if c in df.columns:
                        df = df.rename(columns={c: "Sector"})
                        break
            if "Sector" not in df.columns:
                df["Sector"] = "UNKNOWN"
            df["Symbol"] = df["Symbol"].astype(str).str.upper()
            return df[["Symbol", "Sector"]]
    except Exception:
        pass
    return pd.DataFrame(columns=["Symbol", "Sector"])


def _apply_sector_cap(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies sector caps if enabled in CONFIG.selection.
    Keeps order as-is (assume already ranked by proba desc).
    """
    if df is None or df.empty:
        return df

    sel = (CONFIG or {}).get("selection", {}) if isinstance(CONFIG, dict) else {}
    enabled = bool(sel.get("sector_cap_enabled", False))
    max_per_sector = int(sel.get("max_per_sector", 2))
    max_total = int(sel.get("max_total", 5))

    if not enabled:
        return df.head(max_total)

    sector_map = _load_sector_map()
    if sector_map.empty:
        # No sector info available; just cap total
        return df.head(max_total)

    merged = df.merge(sector_map, on="Symbol", how="left")
    merged["Sector"] = merged["Sector"].fillna("UNKNOWN")

    kept_rows = []
    sector_counts = {}

    for _, row in merged.iterrows():
        sec = row["Sector"]
        cnt = sector_counts.get(sec, 0)
        if cnt < max_per_sector:
            kept_rows.append(row)
            sector_counts[sec] = cnt + 1
        if len(kept_rows) >= max_total:
            break

    if not kept_rows:
        return df.head(max_total)

    out = pd.DataFrame(kept_rows)
    # Drop helper column if present
    if "Sector" in out.columns:
        out = out.drop(columns=["Sector"])
    return out


def _normalize_cols(df: pd.DataFrame, reason_tag: str) -> pd.DataFrame:
    """
    Ensure the output has: Symbol, Entry, SL, Target, proba, Reason.
    - Try to map common alternative names.
    - Fill missing with sensible defaults if needed.
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=["Symbol", "Entry", "SL", "Target", "proba", "Reason"])

    rename_map = {
        # common variants
        "Stop": "SL", "StopLoss": "SL", "stop": "SL",
        "TakeProfit": "Target", "TP": "Target", "take_profit": "Target",
        "Probability": "proba", "score": "proba", "Score": "proba"
    }
    d = df.copy()
    for k, v in rename_map.items():
        if k in d.columns and v not in d.columns:
            d = d.rename(columns={k: v})

    # Create missing required columns with defaults
    for req in ["Symbol", "Entry", "SL", "Target", "proba"]:
        if req not in d.columns:
            if req == "Symbol":
                d["Symbol"] = ""
            elif req == "Entry":
                d["Entry"] = d["Close"] if "Close" in d.columns else 0.0
            elif req == "SL":
                # default 1% cushion
                base = d["Entry"] if "Entry" in d.columns else (d["Close"] if "Close" in d.columns else 0.0)
                d["SL"] = base * 0.99
            elif req == "Target":
                base = d["Entry"] if "Entry" in d.columns else (d["Close"] if "Close" in d.columns else 0.0)
                d["Target"] = base * 1.01
            elif req == "proba":
                d["proba"] = 0.5

    # Reason
    if "Reason" not in d.columns:
        d["Reason"] = reason_tag
    else:
        d["Reason"] = d["Reason"].fillna(reason_tag)

    # Keep only required columns in order
    d = d[["Symbol", "Entry", "SL", "Target", "proba", "Reason"]]
    # Basic cleanups
    d["Symbol"] = d["Symbol"].astype(str).str.upper()
    d = d.dropna(subset=["Symbol"]).reset_index(drop=True)
    # Sort by proba desc if available
    if "proba" in d.columns:
        d = d.sort_values("proba", ascending=False, kind="mergesort")
    return d


def _try_deep_learning(top_k: int) -> Tuple[pd.DataFrame, str]:
    """
    Returns (df, tag). tag in {"dl_ready", "dl_not_ready", "dl_suspended", ...}
    """
    try:
        import dl_runner
        df, tag = dl_runner.predict_topk_if_ready(top_k=top_k)
        if df is not None and not df.empty and tag == "dl_ready":
            df = _normalize_cols(df, "DL-GRU")
            df = _apply_sector_cap(df)
            return df, "dl"
        else:
            # bubble up tag for logging/telemetry if needed by caller
            return pd.DataFrame(), tag or "dl_not_ready"
    except Exception:
        return pd.DataFrame(), "dl_error"


def _try_light_model(top_k: int) -> Tuple[pd.DataFrame, str]:
    """
    Fallback: Light/Robust model (tabular).
    """
    try:
        from model_swing import predict_today
        df = predict_today(top_k=top_k)
        if df is None or df.empty:
            return pd.DataFrame(), "light_empty"
        df = _normalize_cols(df, "LIGHT-ML")
        df = _apply_sector_cap(df)
        return df.head(top_k), "light"
    except Exception:
        return pd.DataFrame(), "light_error"


def choose_and_predict_full(top_k: int = 5) -> Tuple[pd.DataFrame, str]:
    """
    Main entry.
    1) Try DL (only when ready & not suspended).
    2) Fallback to Light/Robust.
    Returns (DataFrame, which_model_tag) where which_model_tag in {"dl","light"}.
    """
    # 1) Deep Learning
    df, tag = _try_deep_learning(top_k=top_k)
    if df is not None and not df.empty and tag == "dl":
        return df.head(top_k), "dl"

    # 2) Fallback
    df2, tag2 = _try_light_model(top_k=top_k)
    if df2 is not None and not df2.empty:
        return df2.head(top_k), "light"

    # Nothing available
    return pd.DataFrame(columns=["Symbol","Entry","SL","Target","proba","Reason"]), "none"


# Optional local test
if __name__ == "__main__":
    out, which = choose_and_predict_full(top_k=int(CONFIG.get("modes", {}).get("auto_top_k", 5)))
    print("Selected model:", which)
    print(out.head(10).to_string(index=False))
