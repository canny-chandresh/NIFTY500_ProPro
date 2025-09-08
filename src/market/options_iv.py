# -*- coding: utf-8 -*-
"""
Light IV fitter placeholder. If you have option chain snapshots,
compute per-underlying median IV and write to datalake/options_chain/iv_summary.parquet
"""

from __future__ import annotations
from pathlib import Path
import pandas as pd
from config import CONFIG

D = Path(CONFIG["paths"]["datalake"]) / "options_chain"

def summarize_iv() -> str:
    files = list(D.glob("*.parquet"))
    if not files:
        (D/"iv_summary.parquet").write_bytes(b"")
        return str(D/"iv_summary.parquet")
    frames=[]
    for f in files:
        try:
            df = pd.read_parquet(f)
            if "impliedVol" in df.columns and "underlying" in df.columns:
                s = df.groupby("underlying")["impliedVol"].median().reset_index()
                frames.append(s)
        except Exception:
            continue
    if frames:
        out = pd.concat(frames).groupby("underlying")["impliedVol"].median().reset_index()
        p = D/"iv_summary.parquet"
        out.to_parquet(p, index=False)
        return str(p)
    return str(D/"iv_summary.parquet")
