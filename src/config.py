# src/config.py
import os

def DL(name: str) -> str:
    base = os.environ.get("DL_BASE", "datalake")
    mapping = {
        "daily_equity": f"{base}/daily_equity.parquet",
        "daily_equity_csv": f"{base}/daily_equity.csv",
        "paper_trades": f"{base}/paper_trades.csv",
        "paper_fills": f"{base}/paper_fills.csv",
        "live_fills": f"{base}/live_fills.csv",
        "sector_map": f"{base}/sector_map.csv",
        "holidays": f"{base}/holidays_nse.csv",
        "ban_list": f"{base}/ban_list.csv",
        "universe": f"{base}/universe.csv"
    }
    return mapping.get(name, f"{base}/{name}.csv")

CONFIG = {
    "features": {
        "regime_v1": True,
        "options_sanity": True,
        "sr_pivots_v1": True,
        "status_cmd": True,
        "reports_v1": True,
        "killswitch_v1": True,
        "drift_alerts": True,
        "walkforward_v1": True
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
    "selection": {
        "sector_cap_enabled": True,
        "sector_cap_k": 2,
        "sector_map_csv": "sector_map.csv"
    },
    "regime": {
        "ema_short": 20,
        "ema_long": 200,
        "breadth_ma": 50,
        "bull_breadth_min": 0.55,
        "bear_breadth_max": 0.45,
        "risk_multipliers": {"bull": 1.0, "neutral": 0.85, "bear": 0.65},
    },
    "smart_money": {
        "min_sms": 0.55,
        "proba_boost": 0.20,
        "weights": {"rvol":0.30,"obv":0.25,"adl":0.20,"rs":0.20,"range":0.05}
    },
    "options": {
        "enabled": True,
        "style": "ATM",
        "min_oi": 10000,
        "min_volume": 1000,
        "min_dte": 1,
        "max_dte": 30,
        "min_rr": 1.5
    },
    "killswitch": {
        "winrate_floor": 0.30,
        "recovery_floor": 0.45,
        "floor_days": 3,
        "recovery_days": 2,
        "cool_off_days": 2
    },
    "drift": {
        "psi_warn": 0.2,
        "psi_alert": 0.3,
        "ref_days": 30,
        "cur_days": 5
    }
}
