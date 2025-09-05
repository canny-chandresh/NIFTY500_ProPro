"""
Global configuration for NIFTY500 Pro Pro Screener.
All feature flags, notification windows, and source settings live here.
"""

CONFIG = {
    # ---- Core features ----
    "features": {
        "regime_v1": True,         # VIX + (optional breadth/NIFTY) + GIFT + News
        "options_sanity": True,    # sanity checks on option strikes/expiries
        "sr_pivots_v1": True,      # S/R, pivots, gap reasoning
        "status_cmd": True,        # /status on Telegram
        "reports_v1": True,        # EOD + periodic reports
        "killswitch_v1": True,     # hit-rate floor protection
        "drift_alerts": True,      # feature drift alerts (light)
        "walkforward_v1": True,    # walk-forward (light)
        "dl_shadow": True,         # Deep Learning shadow training/eval
    },

    # ---- Selection rules ----
    "selection": {
        "sector_cap_enabled": True,   # diversification by sector
        "max_per_sector": 2,
        "max_total": 5
    },

    # ---- Modes (AUTO vs ALGO) ----
    "modes": {
        "auto_enabled": True,          # AUTO = curated 5 → messaged + paper
        "algo_lab_enabled": True,      # ALGO = exploratory many (paper only)
        "auto_top_k": 5,               # Auto sends 5 picks
        "algo_max_trades": 30          # Algo Lab can place up to 30 paper trades
    },

    # ---- Smart money ----
    "smart_money": {
        "proba_boost": 0.05,   # boost if SMS aligned
        "min_sms": 0.45        # minimum SMS score to allow
    },

    # ---- Notifications (IST) ----
    "notify": {
        "send_only_at_ist": True,
        "ist_send_hour": 15,       # recommendations ~3:15 PM IST
        "ist_send_min": 15,
        "window_min": 3,           # ±window for reco messages
        "ist_eod_hour": 17,        # EOD ~5:00 PM IST
        "ist_eod_min": 0,
        "eod_window_min": 15
    },

    # ---- Data freshness & ingestion ----
    "data": {
        "symbols_file": "datalake/symbols.csv",   # optional custom symbol list
        "default_universe": 300,                  # cap symbols for free tier
        "fetch": {
            "daily_days": 400,                    # ~2Y (improves DL)
            "hourly_days": 60,                    # last 60 days for 60m bars
            "minute_days": 5                      # 1m bars (Yahoo limit ~7d)
        }
    },

    # ---- Options ----
    "options": {
        "enabled": True,
        "min_rr": 1.5,          # minimum reward/risk ratio
        "max_sl_pct": 0.25      # stop-loss cap (fraction of entry)
    },

    # ---- Futures ----
    "futures": {
        "enabled": True,
        "lots_default": 1,
        "max_sl_pct": 0.25
    },

    # ---- GIFT Nifty (Yahoo tickers tried in order) ----
    "gift_nifty": {
        "enabled": True,
        "tickers": ["^NSEI","^NSEBANK","NIFTY","NSEI","GIFTNIFTY","GIFTNIFTY.NS"],
        "days": 10
    },

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
        "seq_len": 120,         # last 120 timesteps (hourly)
        "horizon_h": 5,         # predict 5-hour ahead “up” probability
        "max_symbols": 300,     # cap for runner time
        "epochs": 2,            # short shadow training
        "minutes_cap": 3,       # time box per run (minutes)
        "ready_thresholds": {
            "min_symbols": 10,
            "min_epochs": 2,
            "hit_rate": 0.55,
            "brier_max": 0.25
        }
    }
}

if __name__ == "__main__":
    import json
    print(json.dumps(CONFIG, indent=2))
