# -*- coding: utf-8 -*-
"""
config.py — NSE-first data config
"""
from pathlib import Path

CONFIG = {
    # === Paths ===
    "paths": {
        "datalake": "datalake",
        "reports": "reports",
        "models": "models",
    },

    # === Trading Universe (sample; extend this) ===
    "universe": [
        "RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK",
        "AXISBANK", "LT", "SBIN", "KOTAKBANK", "ITC"
    ],

    # === Data sources priority ===
    "data_sources": {
        # Primary equity & options source
        "primary": "nse",           # "nse" | "yahoo"
        "fallback": "yahoo",        # used when primary fails
        "respect_nse_rate_limit": True,
        "nse": {
            "intraday_interval": "5m",
            "max_intraday_symbols_per_run": 60,
            "timeout_sec": 12,
            "retries": 2,
        },
        "yahoo": {
            "daily_period": "5y",   # keep longer history to improve training
            "intraday_interval": "5m",
            "rate_limit_sec": 1.0,
        }
    },

    # === Ingestion knobs (kept for compatibility) ===
    "ingest": {
        "daily_period": "5y",
        "rate_limit_sec": 1.0,
        "intraday": {"max_symbols": 60}
    },

    # === Notifications (Telegram) ===
    "notify": {
        "send_only_at_ist": True,
        "ist_send_hour": 15,   # 3:15pm picks
        "ist_send_min": 15,
        "window_min": 3,
        "ist_eod_hour": 17,    # 5:00pm EOD
        "ist_eod_min": 0,
        "eod_window_min": 10,
    },

    # === Engines toggle ===
    "engines": {
        "ml": {"enabled": True},
        "boosters": {"enabled": True, "models": ["xgb", "lgbm", "cb"]},
        "dl": {
            "enabled": True,
            "ft": {"enabled": True},
            "tcn": {"enabled": True},
            "tst": {"enabled": True},
        },
        "calibration": {"enabled": True},
        "stacker": {"enabled": True},
    },

    # === Kill-switch ===
    "killswitch": {"enabled": True, "min_winrate": 0.30, "days": 3, "cooldown_days": 2},

    # === Discovery ===
    "discovery": {
        "enabled": True,
        "auto_promote": True,
        "min_corr": 0.2,
        "stability_window": 60,
        "max_new_features_per_night": 5,
    },

    # === Features/flags ===
    "features": {
        "regime_v1": True,
        "options_sanity": True,
        "sr_pivots_v1": True,
        "status_cmd": True,
        "reports_v1": True,
        "killswitch_v1": True,
        "drift_alerts": True,
        "walkforward_v1": True,
        "smart_money": True,
        "alpha_runtime": True,
        "alpha_nightly": True,
        "anomaly_iforest": True,
        "news_sentiment": True,
    },

    # === Minimum samples for heavy engines ===
    "min_samples_for_heavy": 2000,
    "min_days_history": 250,

    # === Benchmarks / special symbols ===
    "benchmarks": {
        "nifty50_symbol": "NIFTY50",     # stored in datalake for RS calcs
        "niftybank_symbol": "BANKNIFTY",
        "vix_symbol": "^INDIAVIX"
    },

    # === Options universe (indices + a few stocks) ===
    "options": {
        "enabled": True,
        "indices": ["NIFTY", "BANKNIFTY", "FINNIFTY"],
        "stocks": ["RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK"],
        "max_expiries": 4,       # near expiries to fetch
        "max_strikes_per_side": 12
    },
}
