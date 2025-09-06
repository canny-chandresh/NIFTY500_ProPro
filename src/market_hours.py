# src/market_hours.py
from __future__ import annotations
import os, csv, datetime as dt
from pathlib import Path

IST = dt.timezone(dt.timedelta(hours=5, minutes=30))  # Asia/Kolkata

# NSE standard hours (regular session)
REG_START = dt.time(9, 15)
REG_END   = dt.time(15, 30)

HOLIDAY_CSV = Path("datalake/holidays_nse.csv")

def _now_ist() -> dt.datetime:
    return dt.datetime.now(tz=IST)

def is_holiday(today_ist: dt.date | None = None) -> bool:
    d = today_ist or _now_ist().date()
    if not HOLIDAY_CSV.exists():
        # if not present, assume not holiday (we warn elsewhere)
        return False
    try:
        with HOLIDAY_CSV.open() as f:
            r = csv.DictReader(f)
            for row in r:
                # expects a column 'date' in YYYY-MM-DD
                s = (row.get("date") or row.get("Date") or "").strip()
                if not s: continue
                y,m,dd = [int(x) for x in s.split("-")]
                if dt.date(y,m,dd) == d:
                    return True
    except Exception:
        return False
    return False

def within_regular_hours(now_ist: dt.datetime | None = None) -> bool:
    t = (now_ist or _now_ist()).time()
    return (t >= REG_START) and (t <= REG_END)

def should_run_hourly(now_ist: dt.datetime | None = None) -> bool:
    """
    Gate hourly recommendations: only run on trading weekdays (Mon–Fri),
    not on holidays, and during regular hours.
    """
    now = now_ist or _now_ist()
    if now.weekday() > 4:  # Sat/Sun
        return False
    if is_holiday(now.date()):
        return False
    return within_regular_hours(now)

def is_preopen_window(now_ist: dt.datetime | None = None) -> bool:
    """
    A small pre-open warmup window: 09:00–09:10 IST
    """
    now = now_ist or _now_ist()
    t = now.time()
    return dt.time(9,0) <= t <= dt.time(9,10)

def is_eod_window(now_ist: dt.datetime | None = None) -> bool:
    """
    Post-market summary window: 17:00–17:15 IST
    """
    now = now_ist or _now_ist()
    t = now.time()
    return dt.time(17,0) <= t <= dt.time(17,15)
