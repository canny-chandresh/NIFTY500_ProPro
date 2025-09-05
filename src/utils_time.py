# src/utils_time.py
from __future__ import annotations
import datetime as dt
import calendar
from config import CONFIG

IST_OFFSET = dt.timedelta(hours=5, minutes=30)

def now_ist() -> dt.datetime:
    return dt.datetime.utcnow() + IST_OFFSET

def is_trading_day_ist(d: dt.date | None = None) -> bool:
    d = d or now_ist().date()
    return d.weekday() < 5  # Mon..Fri

def is_trading_hours_ist() -> bool:
    """
    NSE regular session ~09:15–15:30 IST (we pad slightly).
    """
    n = now_ist()
    if not is_trading_day_ist(n.date()):
        return False
    start = n.replace(hour=9,  minute=10, second=0, microsecond=0)
    end   = n.replace(hour=15, minute=40, second=0, microsecond=0)
    return start <= n <= end

def _in_window(n: dt.datetime, hh: int, mm: int, span_min: int) -> bool:
    anchor = n.replace(hour=hh, minute=mm, second=0, microsecond=0)
    start  = anchor - dt.timedelta(minutes=max(0, span_min))
    end    = anchor + dt.timedelta(minutes=max(0, span_min))
    return start <= n <= end

def should_send_now_ist(kind: str = "reco") -> bool:
    n = now_ist()
    notify = CONFIG.get("notify", {})
    if kind == "reco":
        if not notify.get("send_only_at_ist", True):
            return True
        return _in_window(n, int(notify.get("ist_send_hour", 15)),
                             int(notify.get("ist_send_min", 15)),
                             int(notify.get("window_min", 6)))
    if kind == "eod":
        return _in_window(n, int(notify.get("ist_eod_hour", 17)),
                             int(notify.get("ist_eod_min", 0)),
                             int(notify.get("eod_window_min", 10)))
    return False

def is_weekly_window_ist() -> bool:
    """
    Saturday ~17:05 IST window for weekly summary.
    """
    n = now_ist()
    if n.weekday() != 5:  # Saturday
        return False
    return _in_window(n, 17, 5, 20)

def is_month_end_after_hours_ist() -> bool:
    """
    Month-end (calendar) after hours window (~17:10 IST).
    If last day is weekend, run on the last trading weekday after hours.
    """
    n = now_ist()
    year, month = n.year, n.month
    last_dom = calendar.monthrange(year, month)[1]
    last_date = dt.date(year, month, last_dom)

    # If last day on weekend, shift backward to Friday
    while last_date.weekday() > 4:
        last_date -= dt.timedelta(days=1)

    if n.date() != last_date:
        return False
    return _in_window(n, 17, 10, 30)
