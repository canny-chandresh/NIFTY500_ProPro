# -*- coding: utf-8 -*-
"""
entrypoints.py
Top-level orchestration for hourly (3:15 picks), EOD, and periodic reports.
Phase-1 is robust to missing Phase-2 modules (imports wrapped in try/except).
"""

from __future__ import annotations
import os, sys, json, traceback, datetime as dt
from pathlib import Path
from typing import Dict, Any, List

from config import CONFIG
import telegram as tg

# Optional modules (safe imports)
def _try_import(name: str):
    try:
        return __import__(name, fromlist=["*"])
    except Exception:
        return None

feature_store = _try_import("feature_store")
matrix        = _try_import("matrix")
engine_guard  = _try_import("engine_guard")
pipeline_ai   = _try_import("pipeline_ai")
report_eod    = _try_import("report_eod")
report_period = _try_import("report_periodic")

alpha_runtime = _try_import("alpha.runtime")  # may be None in Phase-1

# ---------- Utilities ----------

def _now_ist():
    # IST = UTC+5:30
    return dt.datetime.utcnow() + dt.timedelta(hours=5, minutes=30)

def _in_window(hh: int, mm: int, win_min: int) -> bool:
    now = _now_ist()
    tgt = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
    delta = abs((now - tgt).total_seconds()) / 60.0
    return delta <= float(win_min)

def _time_gated_315pm() -> bool:
    n = CONFIG.get("notify", {})
    if n.get("force_every_run", False):
        return True
    if not n.get("send_only_at_ist", True):
        return True
    return _in_window(n.get("ist_send_hour",15), n.get("ist_send_min",15), n.get("window_min",3))

def _engine_presence_flags() -> dict:
    """Filesystem checks for trained artifacts (used in footer)."""
    base = Path(CONFIG["paths"]["datalake"]) / "features_runtime"
    def ex(p): return (base/p).exists()
    def any_in(p): return (Path(CONFIG["paths"]["datalake"])/p).exists()
    return {
        "booster": ex(Path("boosters")/"xgb.json") or ex(Path("boosters")/"cat.cbm"),
        "dl_ft":   ex(Path("dl_ft")/"ft_transformer.pt"),
        "dl_tcn":  ex(Path("dl_tcn")/"tcn.pt") and any_in("intraday/5m"),
        "dl_tst":  ex(Path("dl_tst")/"tst.pt") and any_in("intraday/5m"),
        "calib":   ex(Path("calibration")/"platt.json") or ex(Path("calibration")/"isotonic.json"),
        "stacker": ex(Path("meta")/"stacker.json"),
    }

def _engine_footer(guard_obj: dict | None = None) -> str:
    ticks = lambda b: "✔" if b else "⚠"
    g = guard_obj.get("engines_active", {}) if isinstance(guard_obj, dict) else {}
    fs = _engine_presence_flags()
    footer = [
        f"ML {ticks(True)}",
        f"Boost {ticks(g.get('booster', fs['booster']))}",
        f"FT {ticks(g.get('dl_ft', fs['dl_ft']))}",
        f"TCN {ticks(g.get('dl_tcn', fs['dl_tcn']))}",
        f"TST {ticks(fs['dl_tst'])}",
        f"Calib {ticks(fs['calib'])}",
        f"Stacker {ticks(fs['stacker'])}",
    ]
    return "Engines: " + " • ".join(footer)

# ---------- Core flows ----------

def daily_update() -> Dict[str, Any]:
    """Light maintenance hook; safe to keep as no-op in Phase-1."""
    out = {"ok": True, "ts": dt.datetime.utcnow().isoformat()+"Z"}
    # Phase-2 can add: refresh_daily_equity/macro; quick validations, etc.
    return out

def hourly_live_or_paper(top_k: int = 5) -> Dict[str, Any]:
    res: Dict[str, Any] = {"ok": True, "ts": dt.datetime.utcnow().isoformat()+"Z", "picks": []}
    try:
        uni = CONFIG.get("universe", [])
        if not feature_store or not matrix or not pipeline_ai:
            res["error"] = "core modules missing (feature_store/matrix/pipeline_ai)"
            return res

        # 1) Build feature frame
        ff = feature_store.get_feature_frame(uni)

        # 2) (Optional) Alpha fast plugins in Phase-1: only if available
        try:
            if alpha_runtime and CONFIG.get("alpha",{}).get("enabled", True):
                ff = alpha_runtime.run_enabled_alphas(ff, fast_only=True)
        except Exception as e:
            print("[alpha] runtime fast error:", e)

        # 3) Matrix
        X, cols, meta, stitched = matrix.build_matrix(ff)

        # 4) Engine guard status (optional)
        guard_obj = {}
        try:
            if engine_guard and hasattr(engine_guard, "ensure_data"):
                guard_obj = engine_guard.ensure_data({"universe": uni})
        except Exception as e:
            print("[guard] error:", e)

        # 5) Score & select
        picks = pipeline_ai.score_and_select(ff, X, cols, meta, top_k=top_k)
        res["picks"] = picks
        res["engine_guard"] = guard_obj

        # 6) 15:15 IST notify
        if picks:
            if _time_gated_315pm():
                lines = pipeline_ai.format_telegram_lines(picks)
                footer = _engine_footer(guard_obj)
                tg.send_recommendations("🕒 15:15 Recos (AI blend)", lines, footer=footer)
                if CONFIG["notify"].get("debug_echo", True):
                    print("[notify] 15:15 sent")
            else:
                if CONFIG["notify"].get("debug_echo", True):
                    print("[notify] skipped 15:15 window; set notify.force_every_run=True to test")
    except Exception as e:
        res["ok"] = False
        res["error"] = repr(e)
        traceback.print_exc()
    return res

def eod_task() -> Dict[str, Any]:
    """
    EOD summary + optional God report. Keeps running even if report modules are absent.
    """
    out = {"ok": True, "ts": dt.datetime.utcnow().isoformat()+"Z"}
    # EOD report
    try:
        if report_eod and hasattr(report_eod, "build_eod"):
            p = report_eod.build_eod()
            out["eod_report"] = p
    except Exception as e:
        out["eod_report_error"] = repr(e)

    # EOD footer ping
    try:
        footer = _engine_footer({})
        tg._send(f"📦 EOD: {footer}", html=False)
    except Exception:
        pass
    return out

def periodic_reports_task() -> Dict[str, Any]:
    """
    Weekly/Monthly summaries if available.
    """
    out = {"ok": True}
    try:
        if report_period and hasattr(report_period, "run_periodic"):
            out["periodic"] = report_period.run_periodic()
    except Exception as e:
        out["periodic_error"] = repr(e)
    return out

def after_run_housekeeping() -> Dict[str, Any]:
    """
    Lightweight cleanup hook; Phase-2 can rotate logs, compact datalake, etc.
    """
    return {"ok": True}
