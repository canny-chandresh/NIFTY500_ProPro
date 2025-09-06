# src/config.py
from __future__ import annotations

# Global configuration for NIFTY500_ProPro
# ------------------------------------------------------------
# Notes:
# - Tweak sizing/fees/market thresholds as you like.
# - Feature flags enable/disable optional modules without code edits.
# - Registry + reports paths are relative to repo root.

CONFIG = {
    # High-level modes
    "modes": {
        "auto_top_k": 5,     # AUTO: curated 5 (your “final 5” recos)
        "algo_top_k": 10,    # ALGO: broader set for learning
    },

    # Feature flags
    "features": {
        "regime_v1": True,
        "options_sanity": True,
        "sr_pivots_v1": True,
        "reports_v1": True,
        "killswitch_v1": True,
        "drift_alerts": True,
        "walkforward_v1": True,
        "explainability": True,
        "news_to_features": True,   # merge news sentiment into features
    },

    # Portfolio sizing & constraints
    "sizing": {
        # Methods: "hrp", "equal", "risk_parity" (your portfolio.py should accept these)
        "auto_method": "hrp",
        "algo_method": "equal",

        # Risk budgets (fractional)
        "auto_total_risk": 1.0,
        "algo_total_risk": 0.5,

        # Per-name clamps (fraction of total)
        "auto_per_name_cap": 0.25,
        "algo_per_name_cap": 0.20,

        # Max allowed turnover per day (fraction of portfolio notional)
        "max_daily_turnover": 0.40,

        # Sector caps (fraction of total; edit as needed)
        "sector_caps": {
            "BANK": 0.35, "IT": 0.30, "PHARMA": 0.30,
            "AUTO": 0.30, "FMCG": 0.30
        },
    },

    # Brokerage / taxes / fees (approx; bps = basis points)
    "fees": {
        "equity":  {"commission_bps": 1.0,  "stt_bps": 10.0, "exchange_bps": 0.3, "gst_bps": 1.8, "sebi_flat": 10.0},
        "futures": {"commission_bps": 0.5,  "stt_bps": 1.0,  "exchange_bps": 0.2, "gst_bps": 1.8, "sebi_flat": 10.0},
        "options": {"commission_bps": 0.0,  "stt_bps": 5.0,  "exchange_bps": 0.2, "gst_bps": 1.8, "sebi_flat": 10.0},
    },

    # Market guardrails and liquidity thresholds
    "market": {
        "equity_circuit_pct": 0.10,         # ±10%
        "fno_circuit_pct":    0.15,         # ±15%
        "min_liquidity_value": 2_00_00_000, # ₹2 Cr notional/day minimum
        "min_option_oi":       50_000,
    },

    # Explainability outputs
    "explain": {
        "top_k_features": 8,
        "export_html": True,
        "save_json": True,
    },

    # Walk-forward evaluation windows (in number of trades)
    "walkforward": {
        "windows": [30, 90, 252],
        "slippage_bps": 5.0,
        "commission_bps": 1.0,
    },

    # News ingestion & mapping -> sentiment features
    "news": {
        "max_items_per_feed": 100,
        "sentiment_weight": 0.2,
        "sector_keyword_map": {
            "BANK":   ["bank","nbfc","psu bank","private bank"],
            "IT":     ["it services","software","saas","infotech"],
            "PHARMA": ["pharma","drug","formulation","api"],
            "AUTO":   ["auto","automobile","ev","vehicle"],
            "FMCG":   ["fmcg","staples","consumption"],
        },
    },

    # Notifications (IST windows verified in workflow scheduling)
    "notify": {
        "send_only_at_ist": True,
        "ist_send_hour": 15,    # 3:15 PM
        "ist_send_min":  15,
        "window_min":    3,     # small window guard
        "ist_eod_hour":  16,    # 4:05 PM EOD recap
        "ist_eod_min":   5,
        "eod_window_min": 7,
    },

    # Corporate actions behavior (if bhavcopy/raw CA files present in datalake/)
    "corp_actions": {
        "apply_on_load": True,        # adjust per-symbol OHLCV on run
        "prefer_bhavcopy": True,      # prefer bhavcopy normalization if uploaded
        "dividends_to_total_return": True,  # add AdjCloseTR series if dividend data available
    },

    # Model registry (lightweight JSON-based)
    "registry": {
        "enabled": True,
        "dir": "reports/registry",
        "keep_last": 20,
    },

    # Deep Learning kill-switch (shadow DL trainer)
    "dl_killswitch": {
        "enabled": True,
        "lookback_runs": 3,        # if last 3 runs < min_winrate -> trip
        "min_winrate_pct": 30.0,   # floor for DL estimated win-rate
        "cooldown_runs": 4,        # pause for next 4 runs after trip
    },
}
