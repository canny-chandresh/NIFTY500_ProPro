from __future__ import annotations
import os, pandas as pd

DL_DIR = "datalake"

def build_hourly_labels(horizons=(1,5,24)) -> str:
    """
    Creates forward-return labels for hourly features table.
    - Binary labels: label_up_{H}h
    - Continuous: ret_fwd_{H}h
    """
    p = os.path.join(DL_DIR,"features_hourly.parquet")
    if not os.path.exists(p):
        return "no_features"
    df = pd.read_parquet(p)
    if df.empty: return "empty"

    df = df.sort_values(["Symbol","Datetime"]).reset_index(drop=True)

    def per_symbol(g):
        g = g.copy()
        for H in horizons:
            fwd = g["Close"].shift(-H) / g["Close"] - 1.0
            g[f"ret_fwd_{H}h"] = fwd
            g[f"label_up_{H}h"] = (fwd > 0).astype(int)
        return g

    out = df.groupby("Symbol", group_keys=False).apply(per_symbol)
    out = out.dropna().reset_index(drop=True)
    outp = os.path.join(DL_DIR,"features_hourly.parquet")  # overwrite with labels added
    out.to_parquet(outp, index=False)
    return outp
