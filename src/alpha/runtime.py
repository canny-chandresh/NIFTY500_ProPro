# -*- coding: utf-8 -*-
import importlib
from pathlib import Path
import pandas as pd
from typing import List
from config import CONFIG
from .registry import ALPHAS

DLAKE = Path(CONFIG["paths"]["datalake"])

def run_enabled_alphas(ff: pd.DataFrame, fast_only: bool = False) -> pd.DataFrame:
    if ff is None or ff.empty:
        return ff
    if not CONFIG.get("alpha",{}).get("enabled", True):
        return ff
    frames = [ff]
    for a in ALPHAS:
        if not a.enabled:
            continue
        if fast_only and not a.fast:
            continue
        try:
            mod = importlib.import_module(f"alpha.factors.{a.module}")
            af = mod.compute(ff.copy(), DLAKE)  # must return alpha_* cols aligned by index
            keep = [c for c in af.columns if c.startswith("alpha_")]
            frames.append(af[keep])
        except Exception as e:
            print(f"[alpha] {a.name} error:", e)
    out = frames[0].join(frames[1:], how="left")
    return out.fillna(0.0)
