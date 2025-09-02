
import os

def DL(name: str) -> str:
    """Resolve datalake file path by logical name."""
    base = os.environ.get("DL_BASE", "datalake")
    mapping = {
        "daily_equity": f"{base}/daily_equity.parquet",
        "intraday_equity": f"{base}/intraday_equity.parquet",
        "futures_daily": f"{base}/futures_daily.parquet",
    }
    return mapping.get(name, f"{base}/{name}.csv")

CONFIG = {
    "data": {
        "bootstrap_days": 60,
        "update_days": 5,
        "universe": "NIFTY500"
    },
    "notify": {
        "send_only_at_ist": True,
        "ist_send_hour": 15,
        "ist_send_min": 15,
        "window_min": 3,
        "ist_eod_hour": 16,
        "ist_eod_min": 5,
        "eod_window_min": 7
    },
    "shadow_learning": {
        "enabled": True,
        "universe_scope": "broad",
        "shadow_universe_cap": 500,
        "slippage_bps": 12
    },
    "selection": {
        "sector_cap_enabled": True,
        "sector_cap_k": 2,
        "sector_map_csv": "sector_map.csv"
    },
    "holiday": {
        "calendar_csv": "holidays_nse.csv",
        "skip_weekends": True
    },
    "telegram_poll": {
        "enabled": True,
        "lookback_min": 15
    },
    "options": {
        "enabled": True,
        "style": "ATM",
        "leverage_k": 8.0,
        "max_loss_cap": 0.5,
        "holding_days": 5
    },
    "regime": {
        "ema_short": 20,
        "ema_long": 200,
        "breadth_ma": 50,
        "bull_breadth_min": 0.55,
        "bear_breadth_max": 0.45,
        "high_vol_thr": 0.30,
        "hysteresis_days": 3,
        "prob_adjustments": {"bull": -0.03, "neutral": 0.0, "bear": 0.05},
        "risk_multipliers": {"bull": 1.0, "neutral": 0.85, "bear": 0.65},
    },
    "smart_money": {
        "min_sms": 0.55,
        "proba_boost": 0.20,
        "weights": {"rvol":0.30,"obv":0.25,"adl":0.20,"rs":0.20,"range":0.05}
    },
    "external_flows": {
        "fii_dii_file": "fii_dii_flows.csv",
        "deals_file": "block_bulk_deals.csv",
        "weights": {"flows": 0.12, "deals": 0.08},
        "deal_fresh_days": 5
    }
}
