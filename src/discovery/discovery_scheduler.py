# -*- coding: utf-8 -*-
"""
Nightly/Weekly coordinator: run discovery, optionally promote top candidates.
Nightly: focuses on ~60–250 trading days
Weekly: invoked by weekly workflow, automatically expands to ~6 months horizon.
"""

from __future__ import annotations
from typing import Dict, Any
from pathlib import Path
import os
from config import CONFIG
from .feature_discovery import run as discover
from .schema_registry import promote

def _is_weekly_context() -> bool:
    # Set by weekly_discovery.yaml implicitly (GitHub Actions name)
    gh_name = os.environ.get("GITHUB_WORKFLOW", "").lower()
    return "weekly" in gh_name

def nightly() -> Dict[str, Any]:
    out = {"ok": True}
    if not CONFIG.get("discovery",{}).get("enabled", True):
        out["skipped"] = True
        return out

    # tell feature_discovery we want longer window if weekly
    Path("reports/discovery").mkdir(parents=True, exist_ok=True)
    if _is_weekly_context():
        (Path("reports/discovery")/"_weekly.flag").write_text("weekly")
    else:
        try:
            (Path("reports/discovery")/"_weekly.flag").unlink()
        except Exception:
            pass

    cand = discover()
    out["found"] = cand
    if CONFIG["discovery"].get("auto_promote", False):
        cols = [c["name"] for c in cand.get("candidates", [])]
        if cols:
            out["promoted_to_spec"] = promote(cols)
    return out
