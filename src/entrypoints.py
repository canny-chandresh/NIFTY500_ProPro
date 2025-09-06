# src/entrypoints.py
from __future__ import annotations
import os, json, datetime as dt

from pipeline_ai import run_auto_and_algo_sessions
from metrics_tracker import summarize_last_n

# optional telegram
try:
    from telegram import send_text
except Exception:
    def send_text(msg: str):
        print("[TELEGRAM Fallback]\n" + msg)

def _stamp():
    return dt.datetime.utcnow().replace(microsecond=0).isoformat()+"Z"

def daily_update(preopen: bool=False):
    """
    Pre-open: refresh regime/GIFT/VIX/news if your other modules handle it.
    Here we just log a heartbeat; your existing modules (regime.py, events.py) can be called from here.
    """
    note = {"when_utc": _stamp(), "phase": "preopen" if preopen else "daily_update"}
    os.makedirs("reports/metrics", exist_ok=True)
    with open("reports/metrics/daily_update.json","w") as f:
        json.dump(note, f, indent=2)

def eod_task():
    """
    End-of-day: finalize reports, send 5pm summary.
    If you already have report_eod.py, call it here. Otherwise send a compact stats message.
    """
    stats = summarize_last_n(days=5)
    msg = (
        "🧾 *EOD Summary*\n"
        f"AUTO → WR: {stats['AUTO']['win_rate']:.2f}, Sharpe: {stats['AUTO']['sharpe']:.2f}\n"
        f"ALGO → WR: {stats['ALGO']['win_rate']:.2f}, Sharpe: {stats['ALGO']['sharpe']:.2f}\n"
        "_(proxy using expected returns; wire your realized P&L for precision)_"
    )
    try: send_text(msg)
    except Exception: pass
    with open("reports/metrics/eod.json","w") as f:
        json.dump({"when_utc": _stamp(), "stats": stats}, f, indent=2)

def periodic_reports_task(kind: str|None=None):
    """
    kind=None → daily aggregate
    kind='weekly' / 'monthly' → coarser rollups (placeholder; extend as needed)
    """
    stats = summarize_last_n(days=30 if kind=="monthly" else 7 if kind=="weekly" else 5)
    with open(f"reports/metrics/aggregate_{kind or 'daily'}.json","w") as f:
        json.dump({"when_utc": _stamp(), "kind": kind or "daily", "stats": stats}, f, indent=2)
    try:
        if kind == "weekly":
            send_text("📊 Weekly report rolled up (see artifact).")
        elif kind == "monthly":
            send_text("📈 Monthly report rolled up (see artifact).")
    except Exception:
        pass

def after_run_housekeeping():
    """
    Light cleanup or alerts; you can add drift checks etc. here.
    """
    hb = {"when_utc": _stamp(), "ok": True}
    with open("reports/metrics/housekeeping.json","w") as f:
        json.dump(hb, f, indent=2)
