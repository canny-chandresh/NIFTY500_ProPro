# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np

def compute(ff: pd.DataFrame, dlake) -> pd.DataFrame:
    out = pd.DataFrame(index=ff.index)
    tv = (ff.get("close",0.0).astype(float) * ff.get("volume",0.0).astype(float))
    z = (tv - tv.mean()) / (tv.std() + 1e-9)
    out["alpha_turnover_liquidity"] = z.clip(-3,3).fillna(0.0)
    return out
