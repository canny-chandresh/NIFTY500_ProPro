# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd

def compute(ff: pd.DataFrame, dlake) -> pd.DataFrame:
    out = pd.DataFrame(index=ff.index)
    gap = ff.get("gap_pct", 0.0).astype(float)
    atr = (ff.get("atr_pct", 0.0).astype(float)).replace(0, np.nan)
    close_in_gap = ff.get("close_in_gap", 0.0).astype(float)
    score = (np.sign(gap) * close_in_gap) / atr
    out["alpha_gap_decay"] = score.replace([np.inf, -np.inf, np.nan], 0.0).clip(-5,5)
    return out
