# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
import pandas as pd
import numpy as np
from config import CONFIG

D = Path(CONFIG["paths"]["datalake"]) / "intraday" / "5m"

def _orb_strength(sym: str, first_min=30) -> float:
    fp = D / f"{sym}.csv"
    if not fp.exists():
        return 0.0
    try:
        df = pd.read_csv(fp)
        if df.empty: return 0.0
        df["datetime"] = pd.to_datetime(df["datetime"])
        day = df["datetime"].dt.date.max()
        today = df[df["datetime"].dt.date == day].copy()
        if today.empty: return 0.0
        # ORB = first 30 minutes
        start = today["datetime"].min()
        brk = start + pd.Timedelta(minutes=first_min)
        orb = today[today["datetime"] <= brk]
        rest = today[today["datetime"] > brk]
        if orb.empty or rest.empty: return 0.0
        hi, lo = orb["high"].max(), orb["low"].min()
        last = rest["close"].iloc[-1]
        rng = (hi - lo) or 1e-6
        # normalize: +1 if above high, -1 if below low, scaled
        if last > hi:
            return float(min(1.0, (last - hi) / rng))
        if last < lo:
            return float(max(-1.0, (last - lo) / rng))
        return 0.0
    except Exception:
        return 0.0

def compute(ff: pd.DataFrame, dlake) -> pd.DataFrame:
    out = pd.DataFrame(index=ff.index)
    vals = []
    for sym in ff["symbol"]:
        vals.append(_orb_strength(sym))
    out["alpha_orb_breakout"] = vals
    return out
