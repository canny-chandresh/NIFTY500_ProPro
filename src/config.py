# -*- coding: utf-8 -*-
"""
config.py
Single source of truth for runtime flags, paths, universe, schedules, risk, notify,
ingest, alpha/discovery, and hygiene controls. Other modules must import CONFIG.
"""

from __future__ import annotations
from pathlib import Path

# ---------- Paths ----------
ROOT = Path(".")
DATA_DIR = ROOT / "datalake"
REPORTS_DIR = ROOT / "reports"

CONFIG = {
    "paths": {
        "root": str(ROOT.resolve()),
        "datalake": str(DATA_DIR),
        "reports": str(REPORTS_DIR),
    },

    # ---------- Universe ----------
    # Keep this lean; you can expand in nightly to avoid rate-limits in day runs.
    "universe": [
        "RELIANCE","TCS","INFY","HDFCBANK","ICICIBANK",
        "SBIN","HINDUNILVR","LT","ITC","AXISBANK",
        # add more symbols as you like
    ],

    # ---------- Feature Flags (core) ----------
    "features": {
        "regime_v1": True,
        "sr_pivots_v1": True,
        "options_sanity": True,
        "reports_v1": True,
        "killswitch_v1": True,
        "walkforward_v1": True,
        "drift_alerts": True,
        "status_cmd": True,
    },

    # ---------- Notification / Schedules ----------
    "notify": {
        # 3:15 PM IST picks window (send only within ±window_min)
        "send_only_at_ist": True,
        "ist_send_hour": 15,
        "ist_send_min": 15,
        "window_min": 3,
        # End-of-day summary ~16:05 IST
        "ist_eod_hour": 16,
        "ist_eod_min": 5,
        "eod_window_min": 7,
        # For testing: force to send every run (overrides window guard)
        "force_every_run": False,
        # Debug echo to logs
        "debug_echo": True,
    },

    # ---------- Ingest (Phase-2 extractors will read this; safe to keep here) ----------
    "ingest": {
        "rate_limit_sec": 1.2,
        "daily": {"enabled": True, "lookback_days": 750},  # ~3Y for day runs
        "intraday": {"enabled": True, "bar": "5m", "max_symbols": 200, "period": "5d"},
        "options": {"enabled": True, "underlyings": ["NIFTY","BANKNIFTY"], "snap_expiries": 2, "max_strikes": 15},
        "macro": {
            "enabled": True,
            "tickers": {
                "india_vix": "^INDIAVIX",
                "dxy": "DX-Y.NYB",
                "us10y": "^TNX",
                "wti": "CL=F",
                "gold": "GC=F",
                "usdinr": "USDINR=X",
                "gift_nifty": "^GIFNIFTY"
            }
        }
    },

    # ---------- Matrix spec ----------
    # matrix.py will auto-create this file with defaults if missing.
    "feature_spec_file": str(DATA_DIR / "feature_spec.yaml"),

    # ---------- Alpha layer (plugins) ----------
    "alpha": {
        "enabled": True,
        "shadow_only": True,           # start in shadow; promotions happen after nightly checks
        "fast_hourly": ["alpha_gap_decay"],
        "heavy_nightly": ["alpha_pair_flow","alpha_event_guard"],
        "promotion": {
            "min_days": 10,
            "min_ic": 0.03,
            "max_dd_pct": 8.0,
            "review_required": True
        }
    },

    # ---------- Risk / Sizing (used by pipeline_ai) ----------
    "risk": {
        "kelly_fraction": 0.25,        # Kelly-lite
        "max_notional_per_trade": 200000.0,
        "min_notional_per_trade": 20000.0,
        "per_trade_var_cap_pct": 0.02,
        "slippage_bps": 8.0,
        "fees_bps": 3.0,
        "atr_stop_mult": 1.2,          # can be adjusted by ATR policy
        "atr_target_mult": 2.0,
    },

    # ---------- Kill switch ----------
    "killswitch": {
        "enabled": True,
        "hit_rate_floor_pct": 30.0,      # if < 30% for N consecutive days -> pause signals
        "consecutive_days": 3,
        "cooldown_days": 2
    },

    # ---------- Discovery (feature R&D) ----------
    "discovery": {
        "enabled": True,
        "sources": {
            "news": [],                  # add RSS/API urls to activate
        },
        "max_new_features_per_night": 3,
        "auto_promote": False,           # keep manual review by default
        "safety": {"leak_checks": True}
    },

    # ---------- Diagnostics / Hygiene ----------
    "diagnostics": {
        "echo_pythonpath": False
    },

    # ---------- Auto bug fixer ----------
    "autofix": {
        "enabled": True,
        "dry_run": False,
        "write_issue": False
    },
}
