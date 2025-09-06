# src/entrypoints.py
from __future__ import annotations
import os, json, datetime as dt
from error_logger import RunLogger
from metrics_tracker import summarize_last_n

try:
    from telegram import send_text, send_stats
except Exception:
    def send_text(msg: str): print("[TELEGRAM Fallback]\n"+msg)
    def send_stats(stats: dict, title: str="Summary"): 
        print("[TELEGRAM Fallback] "+title); print(stats)

def _stamp(): return dt.datetime.utcnow().replace(microsecond=0).isoformat()+"Z"

def daily_update(preopen: bool=False):
    logger = RunLogger(label="preopen" if preopen else "daily_update")
    with logger.capture_all("daily_update", swallow=True):
        note = {"when_utc": _stamp(), "phase": "preopen" if preopen else "daily_update"}
        os.makedirs("reports/metrics", exist_ok=True)
        with open("reports/metrics/daily_update.json","w") as f:
            json.dump(note, f, indent=2)
        send_text("⏰ Pre-open warm-up complete." if preopen else "🔁 Daily update done.")
    logger.dump()

def eod_task():
    logger = RunLogger(label="eod")
    with logger.capture_all("eod", swallow=True):
        stats = summarize_last_n(days=5)
        send_stats(stats, title="🧾 EOD Summary")
        with open("reports/metrics/eod.json","w") as f:
            json.dump({"when_utc": _stamp(), "stats": stats}, f, indent=2)
    logger.dump()

def periodic_reports_task(kind: str|None=None):
    logger = RunLogger(label=f"periodic_{kind or 'daily'}")
    with logger.capture_all("periodic", swallow=True):
        days = 30 if kind=="monthly" else 7 if kind=="weekly" else 5
        stats = summarize_last_n(days=days)
        with open(f"reports/metrics/aggregate_{kind or 'daily'}.json","w") as f:
            json.dump({"when_utc": _stamp(), "kind": kind or "daily", "stats": stats}, f, indent=2)
        if kind == "weekly": send_text("📊 Weekly report rolled up.")
        elif kind == "monthly": send_text("📈 Monthly report rolled up.")
    logger.dump()

def after_run_housekeeping():
    logger = RunLogger(label="housekeeping")
    with logger.capture_all("housekeeping", swallow=True):
        hb = {"when_utc": _stamp(), "ok": True}
        with open("reports/metrics/housekeeping.json","w") as f:
            json.dump(hb, f, indent=2)
    logger.dump()
