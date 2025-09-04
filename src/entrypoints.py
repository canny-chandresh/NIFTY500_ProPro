# src/entrypoints.py
from __future__ import annotations
import os, json, pandas as pd
from telegram import send_message
from report_eod import build_eod
from report_periodic import build_periodic
from kill_switch import evaluate_and_update
from utils_time import should_send_now_ist
from livefeeds import refresh_equity_data

def daily_update():
    print("daily_update(): pulling live equities via yfinance…")
    os.makedirs("reports", exist_ok=True)
    info = refresh_equity_data(days=60, interval="1d")
    # stash source usage (equities)
    sources = {"equities": info}
    # if options_source.txt exists from pipeline, include it later there
    path = "reports/sources_used.json"
    if os.path.exists(path):
        try:
            prev = json.load(open(path))
            prev.update(sources)
            sources = prev
        except Exception:
            pass
    json.dump(sources, open(path, "w"), indent=2)
    print("daily_update():", sources)
    return True

def eod_task():
    print("eod_task(): building EOD report…")
    os.makedirs("reports", exist_ok=True)
    build_eod()
    return True

def periodic_reports_task():
    print("periodic_reports_task(): building periodic reports…")
    os.makedirs("reports", exist_ok=True)
    build_periodic()
    return True

def after_run_housekeeping():
    print("after_run_housekeeping(): evaluating kill-switch…")
    evaluate_and_update()
    return True

def send_5pm_summary():
    if not should_send_now_ist(kind="eod"):
        print("send_5pm_summary(): outside EOD window; skipping.")
        return False

    def exists(p): return os.path.exists(p)
    def head(p, n=5):
        try: return pd.read_csv(p).head(n)
        except: return None

    eq = "datalake/paper_trades.csv"
    op = "datalake/options_paper.csv"
    fu = "datalake/futures_paper.csv"

    parts = ["*NIFTY500 Pro Pro — 5:00 PM Summary*"]
    if exists(eq):
        df = head(eq, 5)
        parts.append(f"• Equity paper trades: {len(pd.read_csv(eq))}")
        if df is not None:
            parts.append("  _Latest Equity samples:_")
            for r in df.itertuples():
                parts.append(f"  - {r.Symbol} @ {getattr(r,'Entry',0):.2f}  SL {getattr(r,'SL',0):.2f}  Tgt {getattr(r,'Target',0):.2f}")
    else:
        parts.append("• Equity paper trades: 0")

    if exists(op):
        df = head(op, 5)
        parts.append(f"• Options paper trades: {len(pd.read_csv(op))}")
        if df is not None:
            parts.append("  _Latest Options samples:_")
            for r in df.itertuples():
                parts.append(f"  - {r.Symbol} {r.Leg} {getattr(r,'Strike',0)} {getattr(r,'Expiry',None)} LTP {getattr(r,'EntryPrice',0):.2f} RR {getattr(r,'RR',0) or 0:.2f}")
    else:
        parts.append("• Options paper trades: 0")

    if exists(fu):
        df = head(fu, 5)
        parts.append(f"• Futures paper trades: {len(pd.read_csv(fu))}")
        if df is not None:
            parts.append("  _Latest Futures samples:_")
            for r in df.itertuples():
                parts.append(f"  - {r.Symbol} @ {getattr(r,'EntryPrice',0):.2f}  SL {getattr(r,'SL',0):.2f}  Tgt {getattr(r,'Target',0):.2f}")
    else:
        parts.append("• Futures paper trades: 0")

    text = "\n".join(parts)
    try:
        send_message(text)
    except Exception as e:
        print("Telegram EOD send failed:", e)
        return False
    return True
