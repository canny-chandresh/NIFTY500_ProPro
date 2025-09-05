# src/config.py
from __future__ import annotations

CONFIG = {
    # ---- Feature flags (already used across your repo) ----
    "features": {
        "regime_v1": True,
        "sr_pivots_v1": True,
        "options_sanity": True,
        "status_cmd": True,
        "reports_v1": True,
        "killswitch_v1": True,
        "drift_alerts": True,
        "walkforward_v1": True,
    },

    # ---- Selection / caps ----
    "selection": {
        "sector_cap_enabled": True,
        "max_per_sector": 2,
        "max_total": 5,
        "force_top5": False,           # keep False for safety
        "min_backfill_proba": 0.50,
    },

    # ---- Modes (how many picks each engine aims for) ----
    "modes": {
        "auto_top_k": 5,
        "algo_top_k": 10,              # ALGO explores more candidates
    },

    # ---- Paper trading cost model (bps round trip) ----
    "paper_costs": {
        "equity_bps": 3.0,
        "options_bps": 30.0,
        "futures_bps": 5.0,
    },

    # ---- Notifications (Telegram windows, IST) ----
    "notify": {
        "send_only_at_ist": True,
        "ist_send_hour": 15,
        "ist_send_min": 15,
        "window_min": 3,
        "ist_eod_hour": 17,
        "ist_eod_min": 0,
        "eod_window_min": 10,
    },

    # ---- Options runtime cfg (placeholder; keep as-is) ----
    "options": {
        "enabled": True,
        "min_iv": 0.0,
        "max_spread_pct": 2.0
    },

    # ---- Live trading toggles (SAFE DEFAULTS) ----
    "live": {
        "dry_run": True,               # hard safety: never place real orders if True
        "enable_auto_live": False,     # AUTO live? (False by default)
        "enable_algo_live": False,     # allow ALGO to ever go live (False by default)
        "conditional_algo_live": True, # AI can *request* ALGO live if True (still respects enable_algo_live)
        "broker": {
            "provider": "zerodha",     # or "stub"
            "api_key": "",
            "api_secret": "",
            "user_id": "",
            "access_token": "",        # leave blank until you wire broker
        },
        # AI conditions for ALGO live unlock
        "algo_live_rules": {
            "auto_wr_min": 0.65,       # need AUTO win-rate >= 65% over window
            "vix_max": 14.0,           # low volatility
            "regimes_ok": ["bull", "neutral"],
            "max_extra_trades": 3,     # how many ALGO trades can be live
            "per_trade_cap": 0.10,     # 10% of exposure cap per trade
            "min_proba": 0.54          # require at least this confidence
        }
    }
}
