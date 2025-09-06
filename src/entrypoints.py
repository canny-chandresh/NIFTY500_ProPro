# src/entrypoints.py
from __future__ import annotations
import os, json, datetime as dt
import pandas as pd

from error_logger import RunLogger
from locks import RunLock
from market_hours import should_run_hourly, is_preopen_window, is_eod_window
from metrics_tracker import summarize_last_n
from archiver import run_archiver
from backtester import backtest_trades

# Telegram fallbacks
try:
    from telegram import send_text, send_stats
except Exception:
    def send_text(msg: str): print("[TELEGRAM Fallback]\n"+msg)
    def send_stats(stats: dict, title: str="Summary"):
        print("[TELEGRAM Fallback] "+title); print(stats)

# Pipeline runner (AUTO + ALGO)
try:
    from pipeline_ai import run_auto_and_algo_sessions
except Exception:
    def run_auto_and_algo_sessions(*a, **k):
        print("[PIPELINE Fallback] run_auto_and_algo_sessions not available")
        return 0, 0

DL = "datalake"

def _stamp() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat()+"Z"

# ----------------- Pre-open / Daily update -----------------

def daily_update(preopen: bool=False):
    """
    Lightweight prep. If preopen=True (first run before open), it just warms up and pings TG.
    """
    label = "preopen" if preopen else "daily_update"
    logger = RunLogger(label=label)
    with logger.capture_all(label, swallow=True):
        note = {"when_utc": _stamp(), "phase": label}
        os.makedirs("reports/metrics", exist_ok=True)
        with open(f"reports/metrics/{label}.json","w", encoding="utf-8") as f:
            json.dump(note, f, indent=2)
        send_text("⏰ Pre-open warm-up complete." if preopen else "🔁 Daily update done.")
    logger.dump()

# ----------------- Main recommendation session -----------------

def run_paper_session(top_k: int = 5):
    """
    Hourly recommendation pass with market-hours gating & idempotency lock.
    """
    # Gate: only during trading hours (IST) — allow if preopen/eod windows
    if not (should_run_hourly() or is_preopen_window() or is_eod_window()):
        print("[gate] Outside trading window; skipping.")
        return

    logger = RunLogger(label="reco_session")
    with logger.capture_all("reco_session", swallow=True):
        try:
            with RunLock("reco_session", ttl_sec=1800):
                a, b = run_auto_and_algo_sessions(top_k_auto=top_k, top_k_algo=None)
                send_text(f"✅ Session complete — AUTO:{a} ALGO:{b}")
                logger.add_meta(auto_count=a, algo_count=b)
        except RuntimeError as e:
            print("[lock] another run in progress; skipping:", e)
    logger.dump()

# ----------------- EOD report & housekeeping triggers -----------------

def eod_task():
    """
    End-of-day: run a quick backtest on paper_trades with slippage, push summary,
    roll up metrics, and persist heartbeat.
    """
    logger = RunLogger(label="eod")
    with logger.capture_all("eod", swallow=True):
        # backtest paper trades (if any)
        bt_summary = {}
        p = os.path.join(DL, "paper_trades.csv")
        if os.path.exists(p):
            try:
                df = pd.read_csv(p)
                bt_summary = backtest_trades(df, slippage_bps=5, commission_bps=1)
                print("[backtest] summary:", bt_summary)
            except Exception as e:
                print("[backtest] error:", e)

        # aggregate last 5 days stats
        stats = summarize_last_n(days=5)
        eod_payload = {"when_utc": _stamp(), "stats": stats, "backtest": bt_summary}
        os.makedirs("reports/metrics", exist_ok=True)
        with open("reports/metrics/eod.json","w", encoding="utf-8") as f:
            json.dump(eod_payload, f, indent=2)

        send_stats(stats, title="🧾 EOD Summary")
        if bt_summary:
            send_text(f"🧪 Backtest — trades: {bt_summary.get('trades',0)}, "
                      f"win%: {bt_summary.get('win_rate')}, "
                      f"pnl_sum: {round(bt_summary.get('pnl_sum',0.0),4)}")
    logger.dump()

def periodic_reports_task(kind: str|None=None):
    """
    kind: None/'daily' (default), 'weekly', 'monthly'
    Produces aggregate metrics and pings Telegram.
    """
    label = f"periodic_{kind or 'daily'}"
    logger = RunLogger(label=label)
    with logger.capture_all(label, swallow=True):
        days = 30 if kind=="monthly" else 7 if kind=="weekly" else 5
        stats = summarize_last_n(days=days)
        os.makedirs("reports/metrics", exist_ok=True)
        with open(f"reports/metrics/aggregate_{kind or 'daily'}.json","w", encoding="utf-8") as f:
            json.dump({"when_utc": _stamp(), "kind": kind or "daily", "stats": stats}, f, indent=2)
        if kind == "weekly":
            send_text("📊 Weekly report rolled up.")
        elif kind == "monthly":
            send_text("📈 Monthly report rolled up.")
    logger.dump()

def after_run_housekeeping():
    """
    Post-run cleanup + archiving:
      - rotations & pruning handled by RunLogger.dump()
      - archive datalake files older than 24 months → archives/YYYY-MM.zip
    """
    logger = RunLogger(label="housekeeping")
    with logger.capture_all("housekeeping", swallow=True):
        info = run_archiver(retention_months=24, dry_run=False)
        hb = {"when_utc": _stamp(), "archiver": info}
        os.makedirs("reports/metrics", exist_ok=True)
        with open("reports/metrics/housekeeping.json","w", encoding="utf-8") as f:
            json.dump(hb, f, indent=2)
        print("[archiver]", info)
        send_text("🧹 Housekeeping done (archiver OK).")
    logger.dump()
