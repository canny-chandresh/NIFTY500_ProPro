from __future__ import annotations
import os, json, datetime as dt, calendar
import pandas as pd

RDIR = "reports"

def _load_csv(p): 
    try:
        if os.path.exists(p): return pd.read_csv(p)
    except Exception: pass
    return pd.DataFrame()

def _save(path: str, txt: str):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    open(path, "w").write(txt)

def _period_bounds_week():
    """Mon..Fri of the week that just ended (run on Saturday)."""
    today = dt.datetime.utcnow().date()
    # Go back to Monday
    start = today - dt.timedelta(days=today.weekday() + 1)  # yesterday (Fri) if Sat
    # start_of_week = (start - dt.timedelta(days=4)) if today.weekday()==5 else ...
    # Simpler: last Monday..Friday block
    # Find last Friday:
    last_fri = today - dt.timedelta(days=(today.weekday() - 4) % 7 + 1)
    last_mon = last_fri - dt.timedelta(days=4)
    return (last_mon, last_fri)

def _period_bounds_month():
    """First..last trading-ish day of current month (use calendar month)."""
    today = dt.datetime.utcnow().date()
    first = dt.date(today.year, today.month, 1)
    last_dom = calendar.monthrange(today.year, today.month)[1]
    last = dt.date(today.year, today.month, last_dom)
    return (first, last)

def _slice_by_date(df: pd.DataFrame, start: dt.date, end: dt.date) -> pd.DataFrame:
    if df is None or df.empty: return df
    for c in ("Timestamp","Date","datetime","timestamp"):
        if c in df.columns:
            try:
                d = pd.to_datetime(df[c]).dt.date
                return df[(d >= start) & (d <= end)].copy()
            except Exception:
                continue
    return df

def _fmt(df: pd.DataFrame, cols: list[str], title: str) -> str:
    if df is None or df.empty:
        return f"\n== {title} ==\n(none)\n"
    return f"\n== {title} ==\n" + df[cols].to_string(index=False) + "\n"

def build_periodic():
    os.makedirs(RDIR, exist_ok=True)
    # load
    auto_eq = _load_csv("datalake/paper_trades.csv")
    algo_eq = _load_csv("datalake/algo_paper.csv")
    opts    = _load_csv("datalake/options_paper.csv")
    futs    = _load_csv("datalake/futures_paper.csv")

    # WEEKLY (Sat)
    wmon, wfri = _period_bounds_week()
    w_auto = _slice_by_date(auto_eq, wmon, wfri)
    w_algo = _slice_by_date(algo_eq, wmon, wfri)
    w_opts = _slice_by_date(opts,    wmon, wfri)
    w_futs = _slice_by_date(futs,    wmon, wfri)

    w_txt = [f"NIFTY500 Pro Pro — WEEKLY God Report  ({wmon}..{wfri})"]
    w_txt.append(_fmt(w_auto, ["Timestamp","Symbol","Entry","SL","Target","proba","Reason"], "AUTO (Top 5)"))
    w_txt.append(_fmt(w_algo, ["Timestamp","Symbol","Entry","SL","Target","proba","Reason"], "ALGO Lab"))
    w_txt.append(_fmt(w_opts, ["Timestamp","Symbol","Leg","Strike","Expiry","EntryPrice","SL","Target","RR","Reason"], "Options (paper)"))
    w_txt.append(_fmt(w_futs, ["Timestamp","Symbol","Expiry","EntryPrice","SL","Target","Lots","Reason"], "Futures (paper)"))
    weekly_path = os.path.join(RDIR, "weekly_report.txt")
    _save(weekly_path, "\n".join(w_txt))

    # MONTHLY (month-end after hours; this runs daily but you’ll read at month-end)
    mfirst, mlast = _period_bounds_month()
    m_auto = _slice_by_date(auto_eq, mfirst, mlast)
    m_algo = _slice_by_date(algo_eq, mfirst, mlast)
    m_opts = _slice_by_date(opts,    mfirst, mlast)
    m_futs = _slice_by_date(futs,    mfirst, mlast)

    m_txt = [f"NIFTY500 Pro Pro — MONTHLY God Report  ({mfirst}..{mlast})"]
    m_txt.append(_fmt(m_auto, ["Timestamp","Symbol","Entry","SL","Target","proba","Reason"], "AUTO (Top 5)"))
    m_txt.append(_fmt(m_algo, ["Timestamp","Symbol","Entry","SL","Target","proba","Reason"], "ALGO Lab"))
    m_txt.append(_fmt(m_opts, ["Timestamp","Symbol","Leg","Strike","Expiry","EntryPrice","SL","Target","RR","Reason"], "Options (paper)"))
    m_txt.append(_fmt(m_futs, ["Timestamp","Symbol","Expiry","EntryPrice","SL","Target","Lots","Reason"], "Futures (paper)"))
    monthly_path = os.path.join(RDIR, "monthly_report.txt")
    _save(monthly_path, "\n".join(m_txt))

    return {"weekly": weekly_path, "monthly": monthly_path}
