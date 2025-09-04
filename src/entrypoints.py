from __future__ import annotations
import os, json, traceback
from pipeline import run_paper_session
from utils_time import should_send_now_ist
from livefeeds import refresh_equity_data, refresh_india_vix

# Optional reports (safe imports)
try:
    from report_eod import build_eod
except Exception:
    def build_eod(): return "eod_ok"

try:
    from report_periodic import build_periodic
except Exception:
    def build_periodic(): return "periodic_ok"

def daily_update():
    """
    Pull equities + VIX (yfinance) and merge into reports/sources_used.json.
    Runs on every job; pipeline also self-refreshes if stale.
    """
    os.makedirs("reports", exist_ok=True)
    info = {"equities":{}, "vix":{}}
    try:
        info["equities"] = refresh_equity_data(days=60, interval="1d")
    except Exception as e:
        info["equities"] = {"equities_source": f"error:{type(e).__name__}"}
    try:
        info["vix"] = refresh_india_vix(days=60)
    except Exception as e:
        info["vix"] = {"vix_source": f"error:{type(e).__name__}"}

    path = "reports/sources_used.json"
    if os.path.exists(path):
        try:
            prev = json.load(open(path))
            prev.update(info); info = prev
        except Exception:
            pass
    json.dump(info, open(path, "w"), indent=2)
    print("daily_update():", info)
    return True

def eod_task():
    """
    Build EOD report; optionally gated to 17:00 IST window.
    """
    try:
        if should_send_now_ist(kind="eod"):
            return build_eod()
        else:
            # still build file silently so artifacts exist
            return build_eod()
    except Exception:
        traceback.print_exc()
        return "eod_error"

def periodic_reports_task():
    """
    Build simple aggregates (daily/weekly/monthly) into reports/.
    """
    try:
        return build_periodic()
    except Exception:
        traceback.print_exc()
        return "periodic_error"

def after_run_housekeeping():
    """
    Placeholder for cleanup/rollover if you need it.
    """
    return "ok"

def send_5pm_summary():
    """
    Example: use your telegram module to push EOD summary text if desired.
    Keep as no-op unless you want a message here.
    """
    try:
        import telegram
        if should_send_now_ist(kind="eod"):
            telegram.send_message("EOD summary is ready (see reports bundle).")
            return "sent"
    except Exception:
        pass
    return "skipped"
