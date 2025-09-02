
from datetime import datetime, timezone
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None
from .config import CONFIG

def _now_ist():
    tz = ZoneInfo("Asia/Kolkata") if ZoneInfo else timezone.utc
    return datetime.now(tz)

def should_send_now_ist():
    if not CONFIG.get("notify",{}).get("send_only_at_ist", True):
        return True
    now = _now_ist()
    HH = CONFIG["notify"]["ist_send_hour"]
    MM = CONFIG["notify"]["ist_send_min"]
    W  = CONFIG["notify"]["window_min"]
    if now.hour != HH: return False
    return abs(now.minute - MM) <= W

def should_send_eod_now_ist():
    n = _now_ist()
    HH = CONFIG["notify"]["ist_eod_hour"]
    MM = CONFIG["notify"]["ist_eod_min"]
    W  = CONFIG["notify"]["eod_window_min"]
    if n.hour != HH: return False
    return abs(n.minute - MM) <= W
