from __future__ import annotations
import os, json, traceback
from config import CONFIG
from utils_time import (
    should_send_now_ist, is_trading_hours_ist, is_weekly_window_ist,
    is_month_end_after_hours_ist, is_preopen_window_ist
)
from livefeeds import refresh_equity_data, refresh_india_vix, refresh_gift_nifty
from news_pulse import write_pulse_report
from pipeline import run_paper_session

# Reports
try:
    from report_eod import build_eod
except Exception:
    def build_eod(): return {"txt": "reports/eod_report.txt", "html":"reports/eod_report.html"}

try:
    from report_periodic import build_periodic
except Exception:
    def build_periodic(*args, **kwargs): return {"weekly": None, "monthly": None}

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
    Light run before market opens: refresh GIFT/VIX/News only.
    """
    info = {}
    try:
        if CONFIG.get("gift_nifty", {}).get("enabled", True):
            info["gift"] = refresh_gift_nifty(
                CONFIG.get("gift_nifty", {}).get("tickers", []),
                CONFIG.get("gift_nifty", {}).get("days", 5)
            )
    except Exception as e:
        info["gift_error"] = str(e)

    try:
        info["vix"] = refresh_india_vix(days=int(CONFIG.get("gift_nifty",{}).get("days",5)))
    except Exception as e:
        info["vix_error"] = str(e)

    try:
        if CONFIG.get("news",{}).get("enabled", True):
            info["news"] = write_pulse_report(CONFIG.get("news",{}))
    except Exception as e:
        info["news_error"] = str(e)

    _merge_sources(info)
    print("preopen_primer():", info)
    return True

def daily_update():
    """General refresh: equities + VIX + GIFT + News (idempotent)."""
    info = {"equities":{}, "vix":{}, "gift":{}, "news":{}}
    try: info["equities"] = refresh_equity_data(days=60, interval="1d")
    except Exception as e: info["equities"] = {"equities_source": f"error:{type(e).__name__}"}
    try: info["vix"] = refresh_india_vix(days=int(CONFIG.get("gift_nifty",{}).get("days",5)))
    except Exception as e: info["vix"] = {"vix_source": f"error:{type(e).__name__}"}
    try:
        if CONFIG.get("gift_nifty",{}).get("enabled", True):
            info["gift"] = refresh_gift_nifty(
                CONFIG.get("gift_nifty",{}).get("tickers", []),
                CONFIG.get("gift_nifty",{}).get("days", 5)
            )
    except Exception as e:
        info["gift"] = {"gift_source": f"error:{type(e).__name__}"}
    try:
        if CONFIG.get("news",{}).get("enabled", True):
            info["news"] = write_pulse_report(CONFIG.get("news",{}))
    except Exception as e:
        info["news"] = {"enabled": False, "error": str(e)}
    _merge_sources(info)
    print("daily_update():", info)
    return True

def hourly_job():
    """
    Called every cron tick. We always run 'daily_update' (light),
    but only score/train during trading hours.
    """
    daily_update()
    if is_trading_hours_ist():
        run_paper_session(top_k=int(CONFIG.get("modes",{}).get("auto_top_k",5)))
    return "ok"

def eod_task():
    try:
        res = build_eod()
        # Optional: push a compact digest to Telegram at EOD window
        try:
            import telegram
            msg = (
                "*EOD God Report ready*\n"
                f"- TXT: {res.get('txt')}\n"
                f"- HTML: {res.get('html')}\n"
                "_Includes: AUTO (5), ALGO Lab (exploratory), Options/Futures paper, Shadow metrics._"
            )
            if should_send_now_ist(kind="eod"):
                telegram.send_message(msg)
        except Exception:
            pass
        return res
    except Exception:
        traceback.print_exc(); return "eod_error"

def periodic_reports_task():
    """
    Weekly (Saturday) and Month-end after-hours — handled internally.
    """
    try: return build_periodic()
    except Exception:
        traceback.print_exc(); return {"weekly": None, "monthly": None}

def after_run_housekeeping():
    return "ok"

def send_5pm_summary():
    try:
        import telegram
        if should_send_now_ist(kind="eod"):
            telegram.send_message("EOD summary ready (see reports).")
            return "sent"
    except Exception:
        pass
    return "skipped"
