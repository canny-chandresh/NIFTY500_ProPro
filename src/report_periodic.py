from __future__ import annotations
import os, json, datetime as dt, calendar
import pandas as pd

RDIR="reports"; SH="reports/shadow"

def _csv(p): 
    try:
        if os.path.exists(p): return pd.read_csv(p)
    except Exception: pass
    return pd.DataFrame()
def _j(p):
    try:
        if os.path.exists(p): return json.load(open(p))
    except Exception: pass
    return {}

def _save(path, txt): 
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    open(path,"w").write(txt)

def _week_bounds():
    today = dt.datetime.utcnow().date()
    last_fri = today - dt.timedelta(days=(today.weekday() - 4) % 7 + 1)
    last_mon = last_fri - dt.timedelta(days=4)
    return last_mon, last_fri
def _month_bounds():
    today = dt.datetime.utcnow().date()
    first = dt.date(today.year, today.month, 1)
    last  = dt.date(today.year, today.month, calendar.monthrange(today.year,today.month)[1])
    return first, last

def _slice(df, start, end):
    if df is None or df.empty: return df
    for c in ("Timestamp","Date","Datetime","datetime","timestamp"):
        if c in df.columns:
            try:
                d = pd.to_datetime(df[c]).dt.date
                return df[(d>=start) & (d<=end)].copy()
            except Exception: continue
    return df

def _pnl(df, entry="Entry", tgt="Target", sl="SL"):
    if df is None or df.empty: return {"trades":0,"win":"n/a","pnl":"n/a"}
    wins=(df[tgt]>df[entry]).sum(); wr=wins/max(1,len(df))
    pnl=((df[tgt]-df[entry]).clip(lower=0)+(df[sl]-df[entry]).clip(upper=0)).sum()
    return {"trades":int(len(df)),"win":f"{wr*100:.0f}%","pnl":round(float(pnl),2)}

def build_periodic():
    auto=_csv("datalake/paper_trades.csv"); algo=_csv("datalake/algo_paper.csv")
    opts=_csv("datalake/options_paper.csv"); futs=_csv("datalake/futures_paper.csv")
    dl_hist=_j(os.path.join(SH,"dl_eval_history.json")); dl_hist = dl_hist if isinstance(dl_hist,list) else []

    def _agg_dl(items):
        if not items: return {"entries":0,"avg_hit_rate":"n/a","sum_ret_approx":"n/a"}
        hr = sum(h.get("hit_rate",0) for h in items)/len(items)
        sr = sum((h.get("pnl_proxy",{}) or {}).get("sum_ret",0.0) for h in items)
        return {"entries":len(items),"avg_hit_rate":f"{hr:.2f}","sum_ret_approx":round(float(sr),4)}

    # weekly
    wmon, wfri = _week_bounds()
    w_auto, w_algo, w_opts, w_futs = _slice(auto,wmon,wfri), _slice(algo,wmon,wfri), _slice(opts,wmon,wfri), _slice(futs,wmon,wfri)
    w_dl = [h for h in dl_hist if wmon <= dt.datetime.fromisoformat(h.get("when_utc","").replace("Z","")).date() <= wfri] if dl_hist else []
    w_ml_stats = {"AUTO": _pnl(w_auto), "ALGO": _pnl(w_algo)}
    w_dl_stats = _agg_dl(w_dl)
    w_txt = []
    w_txt.append(f"NIFTY500 Pro Pro — WEEKLY God Report  ({wmon}..{wfri})")
    w_txt.append(f"\n== ML Learning ==\n{json.dumps(w_ml_stats, indent=2)}\n")
    w_txt.append(f"== DL Learning (shadow) ==\n{json.dumps(w_dl_stats, indent=2)}\n")
    _save(os.path.join(RDIR,"weekly_report.txt"), "\n".join(w_txt))

    # monthly
    m1, mN = _month_bounds()
    m_auto, m_algo, m_opts, m_futs = _slice(auto,m1,mN), _slice(algo,m1,mN), _slice(opts,m1,mN), _slice(futs,m1,mN)
    m_dl = [h for h in dl_hist if m1 <= dt.datetime.fromisoformat(h.get("when_utc","").replace("Z","")).date() <= mN] if dl_hist else []
    m_ml_stats = {"AUTO": _pnl(m_auto), "ALGO": _pnl(m_algo)}
    m_dl_stats = _agg_dl(m_dl)
    m_txt = []
    m_txt.append(f"NIFTY500 Pro Pro — MONTHLY God Report  ({m1}..{mN})")
    m_txt.append(f"\n== ML Learning ==\n{json.dumps(m_ml_stats, indent=2)}\n")
    m_txt.append(f"== DL Learning (shadow) ==\n{json.dumps(m_dl_stats, indent=2)}\n")
    _save(os.path.join(RDIR,"monthly_report.txt"), "\n".join(m_txt))

    return {"weekly":"reports/weekly_report.txt", "monthly":"reports/monthly_report.txt"}
