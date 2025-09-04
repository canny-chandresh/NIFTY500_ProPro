# src/config.py
"""
Configuration for NIFTY500 Pro Pro screener.
All tunable parameters are centralized here.
"""

CONFIG = {
    # --- Notifications ---
    "notify": {
        # Recommendation alert (3:15 PM IST)
        "send_only_at_ist": True,
        "ist_send_hour": 15,
        "ist_send_min": 15,
        "window_min": 6,         # 6-minute window (cron safety)

        # End-of-day / summary alert (5:00 PM IST)
        "ist_eod_hour": 17,
        "ist_eod_min": 0,
        "eod_window_min": 10     # 10-minute window
    },

    # --- Options settings ---
    "options": {
        "enabled": True,
        "enable_live_nse": True,        # Pull real NSE option-chain when possible
        "indices": ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"],
        "rr_min": 1.2,                  # Minimum reward/risk filter
        "lot_size": 1,                  # For paper trades
        "ban_list": ["YESBANK", "SUZLON"]  # Ignore noisy names
    },

    # --- Smart money integration ---
    "smart_money": {
        "proba_boost": 0.25,   # How much to tilt probabilities if SMS score > 0.5
        "min_sms": 0.45        # Drop signals below this SMS confidence
    },

    # --- Feature toggles ---
    "features": {
        "regime_v1": True,        # Market regime detection (NIFTY50 + breadth)
        "sr_pivots_v1": True,     # Support/resistance, pivots, EMA gap logic
        "reports_v1": True,       # EOD + periodic reporting enabled
        "killswitch_v1": True,    # Kill-switch if winrate drops
        "drift_alerts": True,     # Data drift alerts
        "walkforward_v1": True    # Walk-forward validation support
    },

    # --- Selection / risk ---
    "selection": {
        "sector_cap_enabled": True,   # Limit picks per sector
        "max_per_sector": 2,
        "fallback_on_empty": True
    },

    # --- Kill switch parameters ---
    "kill_switch": {
        "min_winrate": 0.3,      # Suspend if <30% winrate
        "window_days": 3,        # … for 3 consecutive days
        "recovery_days": 2       # Auto-resume after 2 good days
    }
}
