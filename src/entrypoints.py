from __future__ import annotations
import os, json, traceback
from config import CONFIG
from utils_time import (
    should_send_now_ist, is_trading_hours_ist, is_weekly_window_ist,
    is_month_end_after_hours_ist, is_preopen_window_ist
)

# Live feeds
try:
    from livefeeds import refresh_equity_data, refresh_india_vix, refresh_gift_nifty, refresh_minute_equity
except Exception:
    def refresh_equity_data(*a, **k): return {"equities_source":"unavailable","rows":0}
    def refresh_india_vix(*a, **k): return {"vix_source":"unavailable","rows":0}
    def refresh_gift_nifty(*a, **k): return {"gift_source":"unavailable","rows":0}
    def refresh_minute_equity(*a, **k): return {"equities_source":"unavailable","rows":0}

# Features/Labels
try:
    from features_builder import build_hourly_features
except Exception:
    def build_hourly_features(): return "no_hourly"

try:
    from labels_builder import build_hourly_labels
except Exception:
    def build_hourly_labels(*a, **k): return "no_features"

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

def preopen_primer():
    """
    Pre-open: refresh GIFT/VIX/News (if any external) so regime is primed.
    """
    info = {}
    try:
        if CONFIG.get("gift_nifty",{}).get("enabled", True):
            info["gift"] = refresh_gift_nifty(CONFIG["gift_nifty"]["tickers"], CONFIG["gift_nifty"]["days"])
    except Exception as e:
        info["gift_error"] = str(e)
    try:
        info["vix"] = refresh_india_vix(days=CONFIG.get("gift_nifty",{}).get("days",10))
    except Exception as e:
        info["vix_error"] = str(e)
    _merge_sources(info)
    return True

def daily_update():
    """
    General refresh called on each tick (preopen + hourly + EOD):
    - 1m fetch (last 5–7 days)
    - 60m fetch (last 60 days)
    - 1d fetch (history)
    - build features + labels (hourly)
    """
    info = {"equities":{}, "vix":{}, "gift":{}}
    try:
        info["minute"] = refresh_minute_equity()
    except Exception as e:
        info["minute"] = {"equities_source": f"error:{type(e).__name__}"}
    try:
        info["hourly"] = refresh_equity_data(interval="60m")
    except Exception as e:
        info["hourly"] = {"equities_source": f"error:{type(e).__name__}"}
    try:
        info["daily"] = refresh_equity_data(days=CONFIG["data"]["fetch"]["daily_days"], interval="1d")
    except Exception as e:
        info["daily"] = {"equities_source": f"error:{type(e).__name__}"}
    try:
        info["vix"] = refresh_india_vix(days=CONFIG.get("gift_nifty",{}).get("days",10))
    except Exception as e:
        info["vix"] = {"vix_source": f"error:{type(e).__name__}"}
    try:
        if CONFIG.get("gift_nifty",{}).get("enabled", True):
            info["gift"] = refresh_gift_nifty(CONFIG["gift_nifty"]["tickers"], CONFIG["gift_nifty"]["days"])
    except Exception as e:
        info["gift"] = {"gift_source": f"error:{type(e).__name__}"}

    # Build features + labels for DL
    try:
        featp = build_hourly_features()
        lblp  = build_hourly_labels(horizons=(1,5,24))
        info["features_hourly"] = featp
        info["labels_hourly"] = lblp
    except Exception as e:
        info["features_error"] = str(e)

    _merge_sources(info)
    return True

def hourly_job():
    daily_update()
    if is_trading_hours_ist():
        run_paper_session(top_k=int(CONFIG["modes"].get("auto_top_k",5)))
    # Shadow DL training (time-boxed) happens in workflow "Shadow Lab" step.

def eod_task():
    try:
        res = build_eod()
        # Optional: Telegram note can be sent elsewhere; avoid spamming here
        return res
    except Exception:
        traceback.print_exc(); return "eod_error"

def periodic_reports_task():
    try: return build_periodic()
    except Exception:
        traceback.print_exc(); return {"weekly": None, "monthly": None}

def after_run_housekeeping():
    return "ok"
