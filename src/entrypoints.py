
import json, os, datetime as _dt
from holidays import is_market_closed_today

def daily_update():
    if is_market_closed_today():
        return {"skipped":"holiday/weekend"}
    # placeholder: data refresh would happen here
    return {"ok": True}

def eod_task():
    from .report_eod import build_eod
    return build_eod()

def periodic_reports_task():
    from .report_periodic import build_period
    return {"daily": build_period("D"),
            "weekly": build_period("W"),
            "monthly": build_period("M")}

def after_run_housekeeping():
    try:
        from .telegram import poll_and_respond_status
        poll_and_respond_status()
    except Exception:
        pass
    return True
