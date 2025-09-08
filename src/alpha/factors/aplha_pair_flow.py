# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd

def compute(ff: pd.DataFrame, dlake) -> pd.DataFrame:
    # sector-relative momentum proxy: (close / ema50) scaled by sector average
    out = pd.DataFrame(index=ff.index)
    if "sector" not in ff.columns:
        # if sector mapping not present, fall back to cross-sectional zscore
        base = (ff.get("close", 0.0) / (ff.get("ema50", 1.0))).replace([0, np.inf, -np.inf], 1.0)
        z = (base - base.mean()) / (base.std() + 1e-9)
        out["alpha_pair_flow"] = z.clip(-3, 3).fillna(0.0)
        return out

    df = ff[["symbol","sector","close","ema50"]].copy()
    df["rel"] = (df["close"] / df["ema50"].replace(0, 1.0)).astype(float)
    rel = df.groupby("sector")["rel"].transform(lambda s: (s - s.mean()) / (s.std() + 1e-9))
    out["alpha_pair_flow"] = rel.clip(-3, 3).fillna(0.0).values
    return out
