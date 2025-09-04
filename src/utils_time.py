# src/utils_time.py
from __future__ import annotations
import datetime as dt
from config import CONFIG

def _now_ist():
    # IST = UTC + 5:30
    return dt.datetime.utcnow() + dt.timedelta(hours=5, minutes=30)

def _in_window(now: dt.datetime, hh: int, mm: int, width_min: int) -> bool:
    target = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
    delta = abs((now - target).total_seconds()) / 60.0
    return delta <= max(1, int(width_min))

def should_send_now_ist(kind: str = "reco") -> bool:
    """
    kind:
      - 'reco'  -> use ist_send_hour/ist_send_min/window_min
      - 'eod'   -> use ist_eod_hour/ist_eod_min/eod_window_min
    """
    cfg = CONFIG.get("notify", {})
    now = _now_ist()

    if not cfg.get("send_only_at_ist", True):
        return True

    if kind == "eod":
        hh = int(cfg.get("ist_eod_hour", 17))
        mm = int(cfg.get("ist_eod_min", 0))
        width = int(cfg.get("eod_window_min", 10))
    else:
        hh = int(cfg.get("ist_send_hour", 15))
        mm = int(cfg.get("ist_send_min", 15))
        width = int(cfg.get("window_min", 6))

    return _in_window(now, hh, mm, width)
