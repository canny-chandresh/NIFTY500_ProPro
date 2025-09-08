# src/matrix_audit.py
from __future__ import annotations
import json
from pathlib import Path
from typing import Dict

import pandas as pd
import numpy as np

def run(cfg: Dict) -> Dict:
    rep = Path(cfg.get("paths", {}).get("reports","reports")) / "audit"
    rep.mkdir(parents=True, exist_ok=True)
    out = {"ok": True}

    # 1) Engines registry
    try:
        from engine_registry import list_engines
        out["engines"] = list_engines()
    except Exception as e:
        out["engines_error"] = repr(e)

    # 2) Check blended predictions exist (pipeline_ai step)
    p = Path(cfg.get("paths", {}).get("reports","reports")) / "ai_hourly_status.json"
    if p.exists():
        try:
            js = json.loads(p.read_text())
            out["ai_hourly_topk"] = js.get("top_k")
            out["ranked_rows"] = js.get("ranked_rows")
        except Exception as e:
            out["ai_hourly_parse_error"] = repr(e)
    else:
        out["ai_hourly_missing"] = True

    # 3) UFD features presence
    feat_dir = Path(cfg.get("paths", {}).get("features","datalake/features"))
    ufd_cols_seen = 0
    for f in feat_dir.glob("*_features.csv"):
        try:
            df = pd.read_csv(f, nrows=1)
            ufd_cols_seen += sum([1 for c in df.columns if str(c).startswith("UFD_")])
        except Exception:
            continue
    out["ufd_columns_seen"] = int(ufd_cols_seen)

    # 4) Write result
    (rep / "matrix_audit.json").write_text(json.dumps(out, indent=2), encoding="utf-8")
    return out
