# -*- coding: utf-8 -*-
"""
config.py
Central configuration for NIFTY500 ProPro screener v2.
Edit universes, feature flags, and run-time knobs here.
"""

from pathlib import Path

CONFIG = {
    # === Paths ===
    "paths": {
        "datalake": "datalake",
        "reports": "reports",
        "models": "models",
    },

    # === Trading Universe (customize your stock list) ===
    "universe": [
        "RELIANCE.NS", "TCS.NS", "INFY.NS", "HDFCBANK.NS", "ICICIBANK.NS",
        "AXISBANK.NS", "LT.NS", "SBIN.NS", "KOTAKBANK.NS", "ITC.NS"
    ],

    # === Ingestion knobs ===
    "ingest": {
        "daily_period": "2y",         # backfill horizon
        "rate_limit_sec": 1.0,        # polite throttle to Yahoo
        "intraday": {
            "max_symbols": 60,        # limit intraday symbols/hour
        }
    },

    # === Notifications (Telegram) ===
    "notify": {
        "send_only_at_ist": True,
        "ist_send_hour": 15,      # 3:15pm picks
        "ist_send_min": 15,
        "window_min": 3,
        "ist_eod_hour": 17,       # 5:00pm daily EOD summary
        "ist_eod_min": 0,
        "eod_window_min": 10,
    },

    # === Engines toggle ===
    "engines": {
        "ml": {"enabled": True},                       # base ML
        "boosters": {"enabled": True, "models": ["xgb", "lgbm", "cb"]},
        "dl": {
            "enabled": True,
            "ft": {"enabled": True},                  # FT-Transformer
            "tcn": {"enabled": True},                 # Temporal ConvNet
            "tst": {"enabled": True},                 # TimeSeries Transformer
        },
        "calibration": {"enabled": True},             # Platt/Isotonic
        "stacker": {"enabled": True},                 # Meta-model
    },

    # === Kill-switch and thresholds ===
    "killswitch": {
        "enabled": True,
        "min_winrate": 0.30,          # if <30% for N consecutive days, kill
        "days": 3,
        "cooldown_days": 2,
    },

    # === Discovery engine ===
    "discovery": {
        "enabled": True,
        "auto_promote": True,          # auto add candidates if stable
        "min_corr": 0.2,               # minimum correlation with returns
        "stability_window": 60,        # days for drift/stability
    },

    # === Features & Flags ===
    "features": {
        "regime_v1": True,
        "options_sanity": True,
        "sr_pivots_v1": True,          # Support/Resistance + Gaps
        "status_cmd": True,
        "reports_v1": True,
        "killswitch_v1": True,
        "drift_alerts": True,
        "walkforward_v1": True,
        "smart_money": True,
        "alpha_runtime": True,         # intraday-safe alphas
        "alpha_nightly": True,         # full alpha suite
        "anomaly_iforest": True,       # anomaly detector
        "news_sentiment": True,        # hourly news signal
    },

    # === Minimum samples for heavy engines ===
    "min_samples_for_heavy": 2000,
    "min_days_history": 250,
}
