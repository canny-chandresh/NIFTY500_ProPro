# -*- coding: utf-8 -*-
"""
engine_guard.py
Reports the presence of data slices and engine artifacts for footer/status.
"""

from __future__ import annotations
from pathlib import Path
from typing import Dict, Any
from config import CONFIG

D = Path(CONFIG["paths"]["datalake"])

def ensure_data(ctx: Dict[str, Any]) -> Dict[str, Any]:
    eng = D/"features_runtime"
    def ex(p: Path) -> bool: return p.exists()
    return {
        "data_present": {
            "daily": ex(D/"daily_hot.parquet"),
            "intraday_5m": ex(D/"intraday"/"5m"),
            "macro": ex(D/"macro"/"macro.parquet"),
        },
        "engines_active": {
            "booster": ex(eng/"boosters"/"xgb.json") or ex(eng/"boosters"/"cat.cbm"),
            "dl_ft":   ex(eng/"dl_ft"/"ft_transformer.pt"),
            "dl_tcn":  ex(eng/"dl_tcn"/"tcn.pt"),
            "dl_tst":  ex(eng/"dl_tst"/"tst.pt"),
            "calib":   ex(eng/"calibration"/"platt.json") or ex(eng/"calibration"/"isotonic.json"),
            "stacker": ex(eng/"meta"/"stacker.json"),
        }
    }
