# -*- coding: utf-8 -*-
"""
Global configuration for NIFTY500_ProPro
Paste this file as: src/config.py

Notes:
- Secret tokens (Telegram, broker keys) are read from environment variables.
- All feature/engine/report toggles are here so CI/Actions and Colab share one truth.
- Times below use IST by default for market windows; cron in GitHub runs in UTC.

"""

from __future__ import annotations
import os
from datetime import time

# ---------- Utilities ----------
def env_bool(key: str, default: bool = False) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}

def env_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, default))
    except Exception:
        return default

def env_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, default))
    except Exception:
        return default

# ---------- Core paths ----------
PATHS = {
    "datalake": "datalake",
    "per_symbol": "datalake/per_symbol",
    "features": "datalake/features",
    "intraday_snaps": "datalake/intraday_snaps",
    "options": "datalake/options",
    "flows": "datalake/flows",
    "news": "datalake/news",
    "feature_store": "datalake/feature_store",
    "reports": "reports",
    "explain": "reports/explain",
    "hygiene": "reports/hygiene",
}

# ---------- Engines / Model stack ----------
ENGINES_ACTIVE = [
    # Semi-Pro tier
    "ML_ROBUST",       # ensemble/GBM-classic
    "ALGO_RULES",      # rule-based/technicals
    "AUTO_TOPK",       # portfolio/top-K allocator
    "UFD_PROMOTED",    # unknown-feature discovery (promoted)

    # Pro (shadow challengers)
    "DL_TEMPORAL",     # existing temporal DL (your earlier model_robust/dl)
    "DL_TRANSFORMER",  # transformer time-series engine (shadow)
    "DL_GNN",          # graph neural net (shadow)
]

# ---------- Feature/Indicator controls ----------
FEATURES = {
    "regime_v1": True,           # NIFTY50 + sector breadth regime detection
    "sr_pivots_v1": True,        # support/resistance, pivots, gap logic
    "options_sanity": True,      # options sanity/guardrails
    "graph_weekly": True,        # weekly correlation graph features
    "udf_candidates": True,      # generate unknown feature candidates
    "udf_promoter": True,        # promote to AUTO_* with caps
    "reports_v1": True,          # EOD + periodic reports
    "killswitch_v1": True,       # global kill switch by hit-rate
    "walkforward_v1": True,      # walk-forward backtests
    "drift_alerts": True,        # PSI/KS drift alerts
    "shap_explain": True,        # write SHAP artifacts if model available
    "live_news": True,           # hourly RSS -> sentiment
    "fii_dii_flows": True,       # flows snapshot into datalake/flows
}

# Indicator knobs (used by features_builder / algo rules)
INDICATORS = {
    "ema_fast": 20,
    "ema_slow": 200,
    "atr_period": 14,
    "rsi_period": 14,
    "bb_window": 20,
    "bb_k": 2.0,
}

# Dynamic ATR regime patch (affects stops/targets sizing across ML/DL/AI)
ATR_POLICY = {
    "enable": True,
    # regime buckets map to multipliers on baseline ATR sizing
    "bull": 0.8,
    "neutral": 1.0,
    "bear": 1.2,
    # floor/ceiling to avoid extremes
    "min_mult": 0.6,
    "max_mult": 1.8,
}

# ---------- Selection & portfolio ----------
SELECTION = {
    "top_k": 5,
    "sector_cap_enabled": True,
    "sector_cap_limit": 2,      # max picks per sector inside Top-K
    "min_price": 50.0,
    "max_price": 100000.0,
    "min_liquidity_adv": 2e6,   # not enforced in paper, used in backtest realism
}

# ---------- Risk engine (v2) ----------
RISK = {
    "use_cvar": True,
    "cvar_alpha": 0.05,
    "kelly_blend": 0.5,          # blend fraction with Kelly sizing if enabled
    "max_drawdown_portfolio": 0.25,
    "per_trade_risk_pct": 0.01,  # 1% of equity per signal (paper)
}

# ---------- Kill switch (global safety) ----------
KILL_SWITCH = {
    "enable": True,
    "min_hit_rate_pct": 30.0,     # threshold
    "consecutive_days": 3,        # activate if under threshold N days
    "cooldown_days": 2,           # time off before gradual re-enable
    "auto_recovery": True,        # auto recovery when metrics normalize
}

# ---------- Data sources ----------
DATA = {
    # Equities (Yahoo as primary intraday snapshot in our alt fetcher)
    "equity_intraday_interval": "5m",
    "equity_intraday_lookback_days": 3,

    # Options: NSE default with synthetic fallback
    "options_primary": "NSE",
    "options_symbol_default": "NIFTY",
    "options_synthetic_fallback": True,

    # Futures (placeholder; extend with broker/exchange adapters later)
    "futures_enable": True,

    # Retention
    "retention_months": 24,
    "initial_backfill_days": 60,     # used by one-time backfill/first run
}

# ---------- Reporting windows / market timing (IST) ----------
# These are read by entrypoints/pipeline to gate when to send messages.
NOTIFY = {
    "send_only_at_ist": True,

    # Daily Top-5 recommendation (pre-close) — 15:15 IST
    "ist_send_hour": 15,
    "ist_send_min": 15,
    "window_min": 3,            # tolerance window

    # EOD compile push — 16:05 IST
    "ist_eod_hour": 16,
    "ist_eod_min": 5,
    "eod_window_min": 7,

    # Weekly (Saturday) handled in Actions cron
    # Month-end handled in Actions step
}

# ---------- Telegram / external integrations ----------
TELEGRAM = {
    "enabled": True,
    # read from secrets in Actions or Colab env
    "bot_token_env": "TG_BOT_TOKEN",
    "chat_id_env": "TG_CHAT_ID",
    # message verbosity
    "compact_hourly": True,
    "send_eod_link": True,
}

# ---------- Hygiene / Spec caps ----------
HYGIENE = {
    "feature_cap_total": 400,
    "feature_cap_auto": 120,
    "psi_warn": 0.2,
    "psi_fail": 0.3,
    "weekly_auto_promotions_cap": 10,
}

# ---------- AutoML sweeps ----------
AUTOML = {
    "enable": True,
    "nightly": True,
    "horizons": ["intraday", "swing", "positional"],
    "per_sector_variants": True,
    "max_trials_per_bucket": 20,
    "seed": 42,
}

# ---------- Champion / Challenger ----------
CHAMPION = {
    "auto_switch": False,     # keep False until you’re confident
    "gate": {
        "min_trades": 80,
        "min_weeks": 2,
        "min_hit_rate_delta": 3.0,  # +3pp over champion
        "min_pf_delta": 0.15,
        "drawdown_not_worse": True
    }
}

# ---------- Deep Learning ----------
DL = {
    "temporal_enable": True,
    "transformer_enable": True,   # shadow in engine_dl_transformer.py
    "gnn_enable": True,           # shadow in engine_gnn.py
    "batch_days": 60,             # sliding training window
    "max_epochs": 5,              # keep CI fast; tune on Colab
    "device": "cpu",              # GitHub runners have no GPU
}

# ---------- Unknown Feature Discovery (AUTO_*) ----------
UFD = {
    "candidates_enable": True,
    "promoter_enable": True,
    "p_value_threshold": 0.01,     # ~|t| > 2.58
    "stability_windows": 6,
    "max_promotions_per_week": 10, # mirrored by hygiene weekly cap
}

# ---------- Backtest realism (slippage/liq) ----------
REALISM = {
    "slippage_open_bps": 30,
    "slippage_mid_bps": 10,
    "slippage_close_bps": 20,
    "adv_liquidity_cap": True,
    "roll_cost_bps": 5,           # for futures rolling
}

# ---------- News / Sentiment / Flows ----------
PULSE = {
    "news_enable": True,
    "sentiment_transformers": True,   # uses HF if available; falls back to lexicon
    "fii_dii_enable": True,
}

# ---------- Public config object ----------
CONFIG = {
    "paths": PATHS,
    "engines_active": ENGINES_ACTIVE,
    "features": FEATURES,
    "indicators": INDICATORS,
    "atr_policy": ATR_POLICY,
    "selection": SELECTION,
    "risk": RISK,
    "kill_switch": KILL_SWITCH,
    "data": DATA,
    "notify": NOTIFY,
    "telegram": TELEGRAM,
    "hygiene": HYGIENE,
    "automl": AUTOML,
    "champion": CHAMPION,
    "dl": DL,
    "ufd": UFD,
    "realism": REALISM,
    "pulse": PULSE,
}

# ---------- Environment overrides (optional) ----------
# You can toggle major switches via env without editing the file.
if env_bool("DISABLE_NEWS", False):
    CONFIG["features"]["live_news"] = False
if env_bool("DISABLE_OPTIONS", False):
    CONFIG["data"]["options_primary"] = "NONE"
if env_bool("REDUCE_TOPK", False):
    CONFIG["selection"]["top_k"] = 3

# Telegram can be disabled for forks/tests
if env_bool("DISABLE_TELEGRAM", False):
    CONFIG["telegram"]["enabled"] = False
