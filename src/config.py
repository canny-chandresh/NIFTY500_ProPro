# src/config.py
from __future__ import annotations

CONFIG = {
    # ---- Schedules & notifications ----
    "notify": {
        "send_only_at_ist": True,
        "ist_send_hour": 15, "ist_send_min": 15, "window_min": 3,   # 3:15pm picks
        "ist_eod_hour": 17, "ist_eod_min": 0,  "eod_window_min": 20 # 5:00pm EOD
    },

    # ---- Data sources & live adapters ----
    "data": {
        "equity_live_yahoo": True,          # live (delayed) OHLCV hourly
        "equity_interval": "1h",            # '1h' (or '5m' if available)
        "options_live_nse": True,           # ✅ ON by default (polite fetch)
        "options_fetch_symbols": ["NIFTY","BANKNIFTY"],
        "options_min_interval_min": 15,     # throttle; ≥15m recommended
        "options_fallback_synthetic": True, # fallback to synthetic if blocked
    },

    # ---- Features & auto-features ----
    "features": {
        "regime_v1": True,
        "sr_pivots_v1": True,
        "drift_alerts": True,
        "walkforward_v1": True,
        "reports_v1": True,
        "options_sanity": True,
        "status_cmd": True,
        "killswitch_v1": True,
        "graph_features": True,     # ✅ new
        "risk_engine": True,        # ✅ new
        "auto_feature_factory": True, # ✅ new (self-discovery)
    },

    # ---- Selection & sizing ----
    "selection": {
        "top_k": 5,
        "sector_cap_enabled": True,
        "sector_max_weight": 0.35,
        "hrp_enabled": True
    },

    # ---- Champion / Challenger (AutoML) ----
    "champion": {
        "enabled": True,
        "min_trades": 40,
        "promote_if": {"hit_rate_delta_min": 2.0, "pf_delta_min": 0.10},
        "auto_switch": False  # keep manual until confident
    },

    # ---- Feature store & retention ----
    "feature_store": {
        "enabled": True,
        "retention_days": 730  # 24 months
    },

    # ---- Risk & kill switches ----
    "risk": {
        "var_window": 60,
        "var_q": 0.95,
        "kelly_cap": 0.33,
        "kill_on_drawdown_R": -8.0,     # stop trading if rolling R < -8
        "resume_after_hours": 48
    }
}
