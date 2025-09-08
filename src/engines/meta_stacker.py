# -*- coding: utf-8 -*-
"""
engines/meta_stacker.py
Lightweight stacker stub: combine engine scores with fixed weights now;
nightly job can learn and write weights into datalake/features_runtime/meta/stacker.json
"""

from __future__ import annotations
from pathlib import Path
from typing import Dict, Any, List
import json

from config import CONFIG

BASE = Path(CONFIG["paths"]["datalake"]) / "features_runtime" / "meta"
BASE.mkdir(parents=True, exist_ok=True)

_DEFAULT = {"ml": 0.2, "boost": 0.5, "ft": 0.3, "tcn": 0.2, "tst": 0.3}

def _load_weights() -> Dict[str, float]:
    p = BASE / "stacker.json"
    if p.exists():
        try: return json.loads(p.read_text())
        except Exception: pass
    return dict(_DEFAULT)

def blend(ml: float, boost: float, ft: float, tcn: float, tst: float) -> float:
    w = _load_weights()
    v = (w.get("ml",0)*ml + w.get("boost",0)*boost + w.get("ft",0)*ft +
         w.get("tcn",0)*tcn + w.get("tst",0)*tst)
    denom = sum(abs(w.get(k,0)) for k in ["ml","boost","ft","tcn","tst"]) or 1.0
    return max(0.0, min(1.0, v/denom))
