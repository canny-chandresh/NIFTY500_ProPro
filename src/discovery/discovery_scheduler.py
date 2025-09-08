# -*- coding: utf-8 -*-
"""
Nightly coordinator: run discovery, optionally promote top candidates based on CONFIG.
"""

from __future__ import annotations
from typing import Dict, Any
from config import CONFIG
from .feature_discovery import run as discover
from .schema_registry import promote

def nightly() -> Dict[str, Any]:
    out = {"ok": True}
    if not CONFIG.get("discovery",{}).get("enabled", True):
        out["skipped"] = True
        return out
    cand = discover()
    out["found"] = cand
    if CONFIG["discovery"].get("auto_promote", False):
        cols = [c["name"] for c in cand.get("candidates", [])]
        if cols:
            out["promoted_to_spec"] = promote(cols)
    return out
