# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
import json
from config import CONFIG

def audit():
    d = Path(CONFIG["paths"]["datalake"])
    checks = {
        "daily_hot": (d/"daily_hot.parquet").exists(),
        "intraday_5m_dir": (d/"intraday"/"5m").exists(),
        "macro": (d/"macro"/"macro.parquet").exists(),
        "feature_spec": Path(CONFIG["feature_spec_file"]).exists()
    }
    print(json.dumps(checks, indent=2))
