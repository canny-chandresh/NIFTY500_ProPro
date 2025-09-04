# src/config.py
"""
Central configuration for NIFTY500 Pro Pro.
Tweak values here; all code reads from CONFIG.
"""

CONFIG = {
    # ─────────────────────────────────────────────────────────────────────
    # Notifications & time windows (IST)
    # ─────────────────────────────────────────────────────────────────────
    "notify": {
        # Recommendation alert window (3:15 PM IST)
        "send_only_at_ist": True,
        "ist_send_hour": 15,
        "ist_send_min": 15,
        "window_min": 6,          # widen to tolerate GitHub runner jitter

        # EOD/summary window (5:00 PM IST)
        "ist_eod_hour": 17,
        "ist_eod_min": 0,
        "eod_window_min": 10
    },

    # ─────────────────────────────────────────────────────────────────────
    # Options (NSE live preferred; synthetic fallback is inside options_executor)
    # ─────────────────────────────────────────────────────────────────────
    "options": {
        "enabled": True,
        "enable_live_nse": True,              # try NSE public JSON
        "indices": ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"],
        "rr_min": 1.2,                        # min reward/risk for entries
        "lot_size": 1,                        # paper logging only
        "ban_list": ["YESBANK", "SUZLON"],    # optional noise filter
        # If you want to forbid synthetic fallback, add:
        # "allow_synthetic_fallback": True
    },

    # ─────────────────────────────────────────────────────────────────────
    # Futures (NSE live preferred; synthetic fallback is inside futures_executor)
    # ─────────────────────────────────────────────────────────────────────
    "futures": {
        "enable_live_nse": True,
        "lot_size": 1                          # paper logging only
    },

    # ─────────────────────────────────────────────────────────────────────
    # Smart money gating/boost
    # ─────────────────────────────────────────────────────────────────────
    "smart_money": {
        "proba_boost": 0.25,     # multiply proba by (1 + boost*(sms_score-0.5))
        "min_sms": 0.45          # filter out rows with sms_score below this
    },

    # ─────────────────────────────────────────────────────────────────────
    # Feature toggles
    # ─────────────────────────────────────────────────────────────────────
    "features": {
        "regime_v1": True,       # NIFTY50 regime + breadth
        "sr_pivots_v1": True,    # S/R, pivots, EMA gap, gap-fill reasoning tag
        "reports_v1": True,      # report_eod / report_periodic hooks
        "killswitch_v1": True,   # suspend if winrate < floor for N days
        "drift_alerts": True,    # feature/data drift alerts (if wired)
        "walkforward_v1": True   # walk-forward backtests (if wired)
    },

    # ─────────────────────────────────────────────────────────────────────
    # Selection / risk constraints
    # ─────────────────────────────────────────────────────────────────────
    "selection": {
        "sector_cap_enabled": True,
        "max_per_sector": 2,
        "fallback_on_empty": True
    },

    # ─────────────────────────────────────────────────────────────────────
    # Kill switch parameters
    # ─────────────────────────────────────────────────────────────────────
    "kill_switch": {
        "min_winrate": 0.30,   # suspend when below this
        "window_days": 3,      # for this many consecutive days
        "recovery_days": 2     # resume after this many good days
    }
}


# Convenience: datalake path helpers (optional)
import os
def DL(name: str) -> str:
    """
    Map logical names to datalake files; used by some modules.
    """
    root = "datalake"
    os.makedirs(root, exist_ok=True)
    mapping = {
        "daily_equity": os.path.join(root, "daily_equity.parquet"),
        "daily_equity_csv": os.path.join(root, "daily_equity.csv"),
    }
    return mapping.get(name, os.path.join(root, name))
