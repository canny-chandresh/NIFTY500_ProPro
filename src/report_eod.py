from __future__ import annotations
import os, json, datetime as dt
import pandas as pd

TS = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
RDIR="reports"

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
def _sec(title, body): return f"\n===== {title} =====\n{body.strip()}\n"
def _fmt(df, cols, n=20):
    if df is None or df.empty: return "(none)"
    try: return df[cols].head(n).to_string(index=False)
    except Exception: return df.head(n).to_string(index=False)
def _pnl(df, entry="Entry", tgt="Target", sl="SL"):
    if df is None or df.empty: return {"trades":0,"win":"n/a","pnl":"n/a"}
    wins=(df[tgt]>df[entry]).sum(); wr=wins/max(1,len(df))
    pnl=((df[tgt]-df[entry]).clip(lower=0)+(df[sl]-df[entry]).clip(upper=0)).sum()
    return {"trades":int(len(df)),"win":f"{wr*100:.0f}%","pnl":round(float(pnl),2)}

def build_eod():
    os.makedirs(RDIR, exist_ok=True)
    auto=_csv("datalake/paper_trades.csv"); algo=_csv("datalake/algo_paper.csv")
    opts=_csv("datalake/options_paper.csv"); futs=_csv("datalake/futures_paper.csv")

    sources=_j("reports/sources_used.json"); data_health=_j("reports/data_health.json")
    dl_eval=_j("reports/shadow/dl_eval.json"); dl_kill=_j("reports/shadow/dl_kill_state.json")

    auto_s=_pnl(auto); algo_s=_pnl(algo)

    txt=[]
    txt.append(f"NIFTY500 Pro Pro — EOD God Report  ({TS})")
    txt.append(_sec("DATA HEALTH", json.dumps(data_health, indent=2)))
    txt.append(_sec("AUTO (Top 5 — paper)", f"Stats: {auto_s}\n"+_fmt(auto,["Timestamp","Symbol","Entry","SL","Target","proba","Reason"],20)))
    txt.append(_sec("ALGO Lab (paper, not messaged)", f"Stats: {algo_s}\n"+_fmt(algo,["Timestamp","Symbol","Entry","SL","Target","proba","Reason"],20)))
    txt.append(_sec("OPTIONS (paper)", _fmt(opts,["Timestamp","Symbol","Leg","Strike","Expiry","EntryPrice","SL","Target","RR","Reason"],20)))
    txt.append(_sec("FUTURES (paper)", _fmt(futs,["Timestamp","Symbol","Expiry","EntryPrice","SL","Target","Lots","Reason"],20)))
    txt.append(_sec("DL (shadow) eval", json.dumps(dl_eval, indent=2)))
    txt.append(_sec("DL kill-switch status", json.dumps(dl_kill, indent=2)))
    open(os.path.join(RDIR,"eod_report.txt"),"w").write("\n".join(txt))
    return {"txt":"reports/eod_report.txt", "html":"reports/eod_report.txt"}
