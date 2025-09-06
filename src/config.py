# src/config.py
from __future__ import annotations

CONFIG = {
    "modes": {"auto_top_k": 5, "algo_top_k": 10},

    "features": {
        "regime_v1": True, "options_sanity": True, "sr_pivots_v1": True,
        "reports_v1": True, "killswitch_v1": True, "drift_alerts": True,
        "walkforward_v1": True, "explainability": True, "news_to_features": True,
    },

    # Portfolio sizing & constraints
    "sizing": {
        "auto_method": "hrp", "algo_method": "equal",
        "auto_total_risk": 1.0, "algo_total_risk": 0.5,
        "auto_per_name_cap": 0.25, "algo_per_name_cap": 0.20,
        "max_daily_turnover": 0.40,
        "sector_caps": {"BANK":0.35,"IT":0.30,"PHARMA":0.30,"AUTO":0.30,"FMCG":0.30},
    },

    # Brokerage/fees (approx, free configurable)
    "fees": {
        "equity":  {"commission_bps": 1.0, "stt_bps": 10.0, "exchange_bps": 0.3, "gst_bps": 1.8, "sebi_flat": 10.0},
        "futures": {"commission_bps": 0.5, "stt_bps": 1.0,  "exchange_bps": 0.2, "gst_bps": 1.8, "sebi_flat": 10.0},
        "options": {"commission_bps": 0.0, "stt_bps": 5.0,  "exchange_bps": 0.2, "gst_bps": 1.8, "sebi_flat": 10.0}
    },

    # Market guardrails
    "market": {
        "equity_circuit_pct": 0.10,   # +/-10%
        "fno_circuit_pct": 0.15,
        "min_liquidity_value": 2_00_00_000,   # ₹2 Cr notional/day
        "min_option_oi": 50_000,
    },

    # Explainability
    "explain": {"top_k_features": 8, "export_html": True, "save_json": True},

    # Walk-forward
    "walkforward": {"windows": [30, 90, 252], "slippage_bps": 5.0, "commission_bps": 1.0},

    # News mapping
    "news": {
        "max_items_per_feed": 100, "sentiment_weight": 0.2,
        "sector_keyword_map": {
            "BANK": ["bank","nbfc","psu bank","private bank"],
            "IT": ["it services","software","saas","infotech"],
            "PHARMA":["pharma","drug","formulation","api"],
            "AUTO":["auto","automobile","ev","vehicle"],
            "FMCG":["fmcg","staples","consumption"]
        }
    },

    # Notifications (IST windows, enforced in market_hours)
    "notify": {
        "send_only_at_ist": True, "ist_send_hour": 15, "ist_send_min": 15, "window_min": 3,
        "ist_eod_hour": 16, "ist_eod_min": 5, "eod_window_min": 7
    },

    # Corporate actions (sources & behavior)
    "corp_actions": {
        "apply_on_load": True,     # adjust OHLCV with splits/bonuses
        "prefer_bhavcopy": True,   # prefer NSE bhavcopy if present in datalake/raw/
        "dividends_to_total_return": True
    },

    # Model registry
    "registry": {
        "enabled": True, "dir": "reports/registry", "keep_last": 20
    }
}
