# src/config.py
CONFIG = {
    "model": {
        "mode": "robust",  # "light" | "robust" | "dl"
    },
    "modes": {
        "auto_top_k": 5,
        "algo_top_k": 10,
        "exposure_cap_overall": 1.0,
    },
    "risk": {
        "dynamic": {
            "enable": True,
            "use_atr": True,
            "per_mode": {
                "swing":    {"tp_pct": 0.05, "sl_pct": 0.025, "tp_atr": 3.0, "sl_atr": 1.5},
                "intraday": {"tp_pct": 0.01, "sl_pct": 0.005, "tp_atr": 1.5, "sl_atr": 1.0},
                "futures":  {"tp_pct": 0.03, "sl_pct": 0.015, "tp_atr": 2.5, "sl_atr": 1.2},
                "options":  {"tp_pct": 0.20, "sl_pct": 0.10, "tp_atr": 2.0, "sl_atr": 1.0},
            },
            "vix_adjust": {
                "low_thresh": 12, "high_thresh": 18,
                "low": {"tp_mult": 0.8, "sl_mult": 0.8},
                "high": {"tp_mult": 1.2, "sl_mult": 1.2},
            },
            "regime_adjust": {
                "bull": {"tp_mult": 1.1, "sl_mult": 0.9},
                "bear": {"tp_mult": 0.9, "sl_mult": 1.1},
                "neutral": {"tp_mult": 1.0, "sl_mult": 1.0},
            },
            "clamp": {
                "tp_min_pct": 0.008, "tp_max_pct": 0.08,
                "sl_min_pct": 0.004, "sl_max_pct": 0.04,
            }
        }
    },
    "notify": {
        "send_only_at_ist": True,
        "ist_send_hour": 15,
        "ist_send_min": 15,
        "window_min": 3,
        "ist_eod_hour": 17,
        "ist_eod_min": 5,
        "eod_window_min": 7,
    },
    "live": {
        "dry_run": True,  # flip to False when broker wired
        "enable_algo_live": True,
        "conditional_algo_live": True,
        "algo_live_rules": {
            "auto_wr_min": 0.65,
            "vix_max": 14.0,
            "regimes_ok": ["bull","neutral"],
        }
    }
}
