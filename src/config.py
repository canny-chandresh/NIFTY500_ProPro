# src/config.py
from __future__ import annotations

CONFIG = {
    # ---- Feature flags ----
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

        # If you *really* want exactly 5 every day, flip this True
        # (safer to keep False so the AI can abstain in bad tape)
        "force_top5": False,
        "min_backfill_proba": 0.50,
    },

    # ---- Modes (how many picks per engine) ----
    "modes": {
        "auto_top_k": 5,   # AUTO (top picks)
        "algo_top_k": 10,  # ALGO (exploration)
    },

    # ---- Paper trading costs (round-trip, in bps) ----
    "paper_costs": {
        "equity_bps": 3.0,
        "options_bps": 30.0,
        "futures_bps": 5.0,
    },

    # ---- Notifications (Telegram; IST windows) ----
    "notify": {
        "send_only_at_ist": True,
        "ist_send_hour": 15,
        "ist_send_min": 15,
        "window_min": 3,
        "ist_eod_hour": 17,
        "ist_eod_min": 0,
        "eod_window_min": 10,
    },

    # ---- Options runtime cfg (placeholder) ----
    "options": {
        "enabled": True,
        "min_iv": 0.0,
        "max_spread_pct": 2.0
    },

    # ---- Dynamic risk: TP/SL per mode + ATR + context (VIX/regime) ----
    "risk": {
        "dynamic": {
            "enable": True,       # master switch for dynamic TP/SL
            "use_atr": True,      # prefer ATR×mult if ATR available on the pick
            "atr_lookback": 14,   # for your indicators calc (if you add it upstream)

            # Defaults per trade kind (used if no ATR present or use_atr=False)
            # You asked for 5% / 2.5% — retained for swing; tighter for intraday; wider for options
            "per_mode": {
                "intraday": {"tp_pct": 0.010, "sl_pct": 0.005, "tp_atr": 1.8, "sl_atr": 0.9},
                "swing":    {"tp_pct": 0.050, "sl_pct": 0.025, "tp_atr": 3.0, "sl_atr": 1.5},
                "futures":  {"tp_pct": 0.030, "sl_pct": 0.015, "tp_atr": 2.2, "sl_atr": 1.1},
                "options":  {"tp_pct": 0.200, "sl_pct": 0.100, "tp_atr": None, "sl_atr": None},  # % based
            },

            # Optional: adjust TP/SL when VIX very low or high
            "vix_adjust": {
                "low_thresh": 12.0,
                "high_thresh": 18.0,
                # multipliers applied to % or ATR distances
                "low":  {"tp_mult": 0.95, "sl_mult": 0.95},   # calm → slightly tighter
                "high": {"tp_mult": 1.20, "sl_mult": 1.20},   # volatile → wider
            },

            # Optional: regime tweak (bull / bear / neutral)
            "regime_adjust": {
                "bull":   {"tp_mult": 0.98, "sl_mult": 0.95}, # ride trends; slightly tighter stop
                "bear":   {"tp_mult": 1.05, "sl_mult": 0.95}, # take profits sooner; keep stops honest
                "neutral":{"tp_mult": 1.00, "sl_mult": 1.00},
            },

            # Final clamps to avoid extremes
            "clamp": {
                "tp_min_pct": 0.008, "tp_max_pct": 0.080,   # 0.8% .. 8%
                "sl_min_pct": 0.004, "sl_max_pct": 0.040,   # 0.4% .. 4%
            }
        }
    },

    # ---- Live trading toggles (SAFE DEFAULTS: paper only) ----
    "live": {
        "dry_run": True,               # hard safety: no live orders if True
        "enable_auto_live": False,     # AUTO live? (False by default)
        "enable_algo_live": False,     # allow ALGO to ever go live (False by default)
        "conditional_algo_live": True, # AI can request ALGO live if True
        "broker": {
            "provider": "zerodha",     # or "stub"
            "api_key": "",
            "api_secret": "",
            "user_id": "",
            "access_token": "",
        },
        "algo_live_rules": {
            "auto_wr_min": 0.65,
            "vix_max": 14.0,
            "regimes_ok": ["bull", "neutral"],
            "max_extra_trades": 3,
            "per_trade_cap": 0.10,
            "min_proba": 0.54
        }
    }
}
