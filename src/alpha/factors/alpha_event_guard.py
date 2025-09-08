# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd

def compute(ff: pd.DataFrame, dlake) -> pd.DataFrame:
    # Simple risk-off alpha: penalize when VIX high or recent gap risk is high
    out = pd.DataFrame(index=ff.index)
    vix = ff.get("india_vix", 0.0).astype(float)
    gap = ff.get("gap_pct", 0.0).astype(float).abs()
    atr = ff.get("atr_pct", 0.0).astype(float)
    risk = 0.3*(vix / (vix.rolling(20, min_periods=1).mean()+1e-6)) + 0.7*(gap / (atr+1e-6))
    out["alpha_event_guard"] = (-risk).clip(-5, 0).fillna(0.0)  # negative when risk elevated
    return out
