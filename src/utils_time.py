from __future__ import annotations
import datetime as dt
from config import CONFIG

def _now_ist() -> dt.datetime:
    # IST = UTC+5:30 (no DST)
    return dt.datetime.utcnow() + dt.timedelta(hours=5, minutes=30)

def _in_window(now: dt.datetime, hh: int, mm: int, span_min: int) -> bool:
    anchor = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
    start  = anchor - dt.timedelta(minutes=max(0, span_min))
    end    = anchor + dt.timedelta(minutes=max(0, span_min))
    return start <= now <= end

def should_send_now_ist(kind: str = "reco") -> bool:
    """
    kind='reco' → 15:15 IST window
    kind='eod'  → 17:00 IST window
    """
    now = _now_ist()
    notify = CONFIG.get("notify", {})
    if kind == "reco":
        if not notify.get("send_only_at_ist", True):
            return True
        return _in_window(
            now,
            int(notify.get("ist_send_hour", 15)),
            int(notify.get("ist_send_min", 15)),
            int(notify.get("window_min", 6)),
        )
    if kind == "eod":
        return _in_window(
            now,
            int(notify.get("ist_eod_hour", 17)),
            int(notify.get("ist_eod_min", 0)),
            int(notify.get("eod_window_min", 10)),
        )
    return False
