# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np

def compute(ff: pd.DataFrame, dlake) -> pd.DataFrame:
    out = pd.DataFrame(index=ff.index)
    if "sector" not in ff.columns or ff.empty:
        out["alpha_sector_breadth"] = 0.0
        return out
    # breadth proxy: (close > ema50) share within sector
    tmp = ff[["symbol","sector","close","ema50"]].copy()
    tmp["adv"] = (tmp["close"] > tmp["ema50"]).astype(float)
    br = tmp.groupby("sector")["adv"].transform("mean")
    out["alpha_sector_breadth"] = br.fillna(0.0).values
    return out
