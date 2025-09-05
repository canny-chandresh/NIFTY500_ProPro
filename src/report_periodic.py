from __future__ import annotations
import os, json, datetime as dt, calendar
import pandas as pd

RDIR = "reports"
SHADOW_DIR = "reports/shadow"

def _load_csv(p): 
    try:
        if os.path.exists(p): return pd.read_csv(p)
    except Exception: pass
    return pd.DataFrame()

def _load_json(p):
    try:
        if os.path.exists(p): return json.load(open(p))
    except Exception: pass
    return {}

def _save(path: str, txt: str):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    open(path, "w").write(txt)

def _period_bounds_week():
    today = dt.datetime.utcnow().date()
    # last Monday..Friday (assumes run on Sat)
    last_fri = today - dt.timedelta(days=(today.weekday() - 4) % 7 + 1)
    last_mon = last_fri - dt.timedelta(days=4)
    return (last_mon, last_fri)

def _period_bounds_month():
    today = dt.datetime.utcnow().date()
    first = dt.date(today.year, today.month, 1)
    last_dom = calendar.monthrange(today.year, today.month)[1]
    last = dt.date(today.year, today.month, last_dom)
    return (first, last)

def _slice_by_date(df: pd.DataFrame, start: dt.date, end: dt.date) -> pd.DataFrame:
    if df is None or df.empty: return df
    for c in ("Timestamp","Date","Datetime","datetime","timestamp"):
        if c in df.columns:
            try:
                d = pd.to_datetime(df[c]).dt.date
                return df[(d >= start) & (d <= end)].copy()
            except Exception: continue
    return df

def _fmt(df: pd.DataFrame, cols: list[str], title: str) -> str:
    if df is None or df.empty:
        return f"\n== {title} ==\n(none)\n"
    return f"\n== {title} ==\n" + df[cols].to_string(index=False) + "\n"

def _pnl_stats(df, entry="Entry", tgt="Target", sl="SL"):
    if df is None or df.empty: 
        return {"trades":0, "winrate":"n/a", "pnl":"n/a"}
    wins = (df[tgt] > df[entry]).sum()
    wr = wins / max(1,len(df))
    pnl = ((df[tgt]-df[entry]).clip(lower=0) + (df[sl]-df[entry]).clip(upper=0)).sum()
    return {"trades": int(len(df)), "winrate": f"{wr*100:.0f}%", "pnl": round(float(pnl),2)}

def build_periodic():
    os.makedirs(RDIR, exist_ok=True)

    auto_eq = _load_csv("datalake/paper_trades.csv")
    algo_eq = _load_csv("datalake/algo_paper.csv")
    opts    = _load_csv("datalake/options_paper.csv")
    futs    = _load_csv("datalake/futures_paper.csv")

    # DL eval history for the period
    dl_hist = _load_json(os.path.join(SHADOW_DIR,"dl_eval_history.json"))
    if not isinstance(dl_hist, list): dl_hist = []

    # WEEKLY
    wmon, wfri = _period_bounds_week()
    w_auto = _slice_by_date(auto_eq, wmon, wfri)
    w_algo = _slice_by_date(algo_eq, wmon, wfri)
    w_opts = _slice_by_date(opts,    wmon, wfri)
    w_futs = _slice_by_date(futs,    wmon, wfri)

    w_auto_s = _pnl_stats(w_auto); w_algo_s = _pnl_stats(w_algo)
    w_txt = [f"NIFTY500 Pro Pro — WEEKLY God Report  ({wmon}..{wfri})"]
    w_txt.append(f"\n== ML Learning (AUTO/ALGO) ==\nAUTO: {w_auto_s} | ALGO: {w_algo_s}\n")

    # DL weekly learning: average hit-rate & pnl proxy
    w_dl = [h for h in dl_hist if "when_utc" in h]
    def _to_date(iso): 
        try: return dt.datetime.fromisoformat(iso.replace("Z","")).date()
        except Exception: return None
    w_dl = [h for h in w_dl if (_to_date(h["when_utc"]) or dt.date.min) >= wmon and (_to_date(h["when_utc"]) or dt.date.min) <= wfri]
    if w_dl:
        hr = sum(h.get("hit_rate",0) for h in w_dl)/len(w_dl)
        pnl_sum = sum((h.get("pnl_proxy",{}) or {}).get("sum_ret",0.0) for h in w_dl)
        w_txt.append(f"== DL Learning (shadow) ==\nentries={len(w_dl)} avg_hit_rate={hr:.2f} sum_ret≈{pnl_sum:.4f}\n")
    else:
        w_txt.append("== DL Learning (shadow) ==\n(no entries)\n")

    w_txt.append(_fmt(w_auto, ["Timestamp","Symbol","Entry","SL","Target","proba","Reason"], "AUTO (Top 5)"))
    w_txt.append(_fmt(w_algo, ["Timestamp","Symbol","Entry","SL","Target","proba","Reason"], "ALGO Lab"))
    w_txt.append(_fmt(w_opts, ["Timestamp","Symbol","Leg","Strike","Expiry","EntryPrice","SL","Target","RR","Reason"], "Options (paper)"))
    w_txt.append(_fmt(w_futs, ["Timestamp","Symbol","Expiry","EntryPrice","SL","Target","Lots","Reason"], "Futures (paper)"))
    weekly_path = os.path.join(RDIR, "weekly_report.txt")
    _save(weekly_path, "\n".join(w_txt))

    # MONTHLY
    mfirst, mlast = _period_bounds_month()
    m_auto = _slice_by_date(auto_eq, mfirst, mlast)
    m_algo = _slice_by_date(algo_eq, mfirst, mlast)
    m_opts = _slice_by_date(opts,    mfirst, mlast)
    m_futs = _slice_by_date(futs,    mfirst, mlast)
    m_auto_s = _pnl_stats(m_auto); m_algo_s = _pnl_stats(m_algo)

    m_txt = [f"NIFTY500 Pro Pro — MONTHLY God Report  ({mfirst}..{mlast})"]
    m_txt.append(f"\n== ML Learning (AUTO/ALGO) ==\nAUTO: {m_auto_s} | ALGO: {m_algo_s}\n")
    m_dl = [h for h in dl_hist if (_to_date(h.get("when_utc","")) or dt.date.min) >= mfirst and (_to_date(h.get("when_utc","")) or dt.date.min) <= mlast]
    if m_dl:
        hr = sum(h.get("hit_rate",0) for h in m_dl)/len(m_dl)
        pnl_sum = sum((h.get("pnl_proxy",{}) or {}).get("sum_ret",0.0) for h in m_dl)
        m_txt.append(f"== DL Learning (shadow) ==\nentries={len(m_dl)} avg_hit_rate={hr:.2f} sum_ret≈{pnl_sum:.4f}\n")
    else:
        m_txt.append("== DL Learning (shadow) ==\n(no entries)\n")

    m_txt.append(_fmt(m_auto, ["Timestamp","Symbol","Entry","SL","Target","proba","Reason"], "AUTO (Top 5)"))
    m_txt.append(_fmt(m_algo, ["Timestamp","Symbol","Entry","SL","Target","proba","Reason"], "ALGO Lab"))
    m_txt.append(_fmt(m_opts, ["Timestamp","Symbol","Leg","Strike","Expiry","EntryPrice","SL","Target","RR","Reason"], "Options (paper)"))
    m_txt.append(_fmt(m_futs, ["Timestamp","Symbol","Expiry","EntryPrice","SL","Target","Lots","Reason"], "Futures (paper)"))
    monthly_path = os.path.join(RDIR, "monthly_report.txt")
    _save(monthly_path, "\n".join(m_txt))

    return {"weekly": weekly_path, "monthly": monthly_path}
