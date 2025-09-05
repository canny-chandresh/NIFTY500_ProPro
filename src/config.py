"""
Global configuration for NIFTY500 Pro Pro Screener.
"""

CONFIG = {
    # ---- Core features ----
    "features": {
        "regime_v1": True,
        "options_sanity": True,
        "sr_pivots_v1": True,
        "status_cmd": True,
        "reports_v1": True,
        "killswitch_v1": True,     # (tabular) kill-switch
        "drift_alerts": True,
        "walkforward_v1": True,
        "dl_shadow": True,         # Deep Learning: train/eval in shadow
    },

    # ---- Selection rules ----
    "selection": {"sector_cap_enabled": True, "max_per_sector": 2, "max_total": 5},

    # ---- Modes ----
    "modes": {
        "auto_enabled": True,      # AUTO = curated 5 → messaged + paper
        "algo_lab_enabled": True,  # ALGO = broad exploratory paper trades
        "auto_top_k": 5,
        "algo_max_trades": 30
    },

    # ---- Smart money ----
    "smart_money": {"proba_boost": 0.05, "min_sms": 0.45},

    # ---- Notifications (IST) ----
    "notify": {
        "send_only_at_ist": True,
        "ist_send_hour": 15, "ist_send_min": 15, "window_min": 3,
        "ist_eod_hour": 17, "ist_eod_min": 0, "eod_window_min": 15
    },

    # ---- Data freshness & ingestion ----
    "data": {
        "symbols_file": "datalake/symbols.csv",
        "default_universe": 300,
        "fetch": {
            "daily_days": 400,      # ~2y
            "hourly_days": 60,      # 60d 60m
            "minute_days": 5        # last ~5-7d 1m
        },
        "hygiene": {
            "utc_enforce": True,
            "dedupe": True,
            "sort_keys": ["Symbol","Datetime"],
            "gap_flag": True,         # mark missing bars
            "volume_zero_fill": True, # fill missing minute bars with Vol=0
            "outlier_cap_z": 8.0      # soft cap for absurd OHLC/Vol spikes
        }
    },

    # ---- Options / Futures ----
    "options": {"enabled": True, "min_rr": 1.5, "max_sl_pct": 0.25},
    "futures": {"enabled": True, "lots_default": 1, "max_sl_pct": 0.25},

    # ---- GIFT Nifty & VIX ----
    "gift_nifty": {"enabled": True, "tickers": ["^NSEI","^NSEBANK","GIFTNIFTY","NIFTY","NSEI"], "days": 10},

    # ---- News pulse (RSS) ----
    "news": {
        "enabled": True,
        "feeds": [
            "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
            "https://www.moneycontrol.com/rss/marketreports.xml",
            "https://www.livemint.com/rss/markets",
            "https://www.business-standard.com/rss/markets-106.rss"
        ],
        "keywords_positive": ["upgrade","beats","strong","surge","rally","grow","record","profit"],
        "keywords_negative": ["downgrade","misses","weak","plunge","fall","scam","fraud","default","loss"],
        "lookback_hours": 6,
        "high_risk_threshold": 3
    },

    # ---- Deep Learning (shadow) ----
    "dl": {
        "seq_len": 120,         # 120 hourly steps
        "horizon_h": 5,         # 5-hour forward label
        "max_symbols": 300,
        "epochs": 2,
        "minutes_cap": 3,       # time-box each run (minutes)
        "ready_thresholds": {   # activation gates
            "min_symbols": 10,
            "min_epochs": 2,
            "hit_rate": 0.55,
            "brier_max": 0.25
        },
        # DL-specific kill-switch
        "kill_switch": {
            "window_runs": 6,           # last N eval runs
            "min_test": 200,            # require N test samples for signal
            "hit_rate_floor": 0.48,     # suspend if below floor
            "consec_bad": 3,            # OR if this many bad runs in a row
            "cooloff_hours": 24         # stay suspended for this long
        }
    }
}

if __name__ == "__main__":
    import json; print(json.dumps(CONFIG, indent=2))
