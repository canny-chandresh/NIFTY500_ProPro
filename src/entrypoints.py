from __future__ import annotations
import os, json, traceback
from config import CONFIG
from utils_time import is_trading_hours_ist

# Live feeds
from livefeeds import (
    refresh_equity_minute, refresh_equity_hourly, refresh_equity_daily,
    refresh_india_vix, refresh_gift_nifty
)
from data_quality import run_data_hygiene
from features_builder import build_hourly_features
from labels_builder import build_hourly_labels

# Reports & pipeline
try:
    from report_eod import build_eod
except Exception:
    def build_eod(): return {"txt":"reports/eod_report.txt","html":"reports/eod_report.html"}

try:
    from report_periodic import build_periodic
except Exception:
    def build_periodic(): return {"weekly": None, "monthly": None}

from pipeline import run_paper_session

def _merge_sources(extra: dict):
    os.makedirs("reports", exist_ok=True)
    path = "reports/sources_used.json"
    data = {}
    if os.path.exists(path):
        try: data = json.load(open(path))
        except Exception: data = {}
    data.update(extra or {})
    json.dump(data, open(path, "w"), indent=2)

def daily_update():
    info = {}
    # 1m + 60m + 1d
    try: info["minute"] = refresh_equity_minute()
    except Exception as e: info["minute"] = {"equities_source": f"error:{type(e).__name__}"}
    try: info["hourly"] = refresh_equity_hourly()
    except Exception as e: info["hourly"] = {"equities_source": f"error:{type(e).__name__}"}
    try: info["daily"]  = refresh_equity_daily()
    except Exception as e: info["daily"] = {"equities_source": f"error:{type(e).__name__}"}

    # VIX/GIFT
    try: info["vix"] = refresh_india_vix(days=CONFIG.get("gift_nifty",{}).get("days",10))
    except Exception as e: info["vix"] = {"vix_source": f"error:{type(e).__name__}"}
    try:
        if CONFIG.get("gift_nifty",{}).get("enabled", True):
            info["gift"] = refresh_gift_nifty(CONFIG["gift_nifty"]["tickers"], CONFIG["gift_nifty"]["days"])
    except Exception as e: info["gift"] = {"gift_source": f"error:{type(e).__name__}"}

    # Hygiene → Features → Labels
    try: info["hygiene"] = run_data_hygiene()
    except Exception as e: info["hygiene_error"] = str(e)
    try:
        info["features_hourly"] = build_hourly_features()
        info["labels_hourly"] = build_hourly_labels(horizons=(1,5,24))
    except Exception as e: info["features_error"] = str(e)

    _merge_sources(info)
    return True

def hourly_job():
    daily_update()
    if is_trading_hours_ist():
        run_paper_session(top_k=int(CONFIG["modes"].get("auto_top_k",5)))
    # DL shadow training still happens in workflow step (so we can time-box it safely)

def eod_task():
    try: return build_eod()
    except Exception:
        traceback.print_exc(); return "eod_error"

def periodic_reports_task():
    try: return build_periodic()
    except Exception:
        traceback.print_exc(); return {"weekly": None, "monthly": None}

def after_run_housekeeping(): return "ok"
