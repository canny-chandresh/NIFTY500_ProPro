# src/report_eod.py
import os, pandas as pd, datetime as dt
from config import DL

def _safe_load(fp):
    if os.path.exists(fp):
        try: return pd.read_csv(fp)
        except Exception: pass
    return pd.DataFrame()

def _summary(df):
    if df.empty: return {"trades":0,"winrate":None,"avg_pnl":None}
    trades = len(df)
    wins = (df.get("target_hit",pd.Series([0]*trades))==1).sum()
    winrate = wins / trades if trades else None
    avg_pnl = df.get("pnl_pct", pd.Series([0]*trades)).mean()
    return {"trades":trades,"winrate":winrate,"avg_pnl":avg_pnl}

def build_eod():
    os.makedirs("reports", exist_ok=True)
    eq = _safe_load(DL("paper_fills"))
    op = _safe_load("datalake/options_paper.csv")
    fu = _safe_load("datalake/futures_paper.csv")

    s1, s2, s3 = _summary(eq), _summary(op), _summary(fu)
    f = lambda s: f"{s['winrate']:.2%}" if isinstance(s['winrate'], float) else "NA"

    lines = [
        f"NIFTY500 Pro Pro — EOD Report  ({dt.datetime.now():%Y-%m-%d %H:%M})",
        "",
        f"Equity:  trades={s1['trades']}  winrate={f(s1)}  avgPnL={s1['avg_pnl']}",
        f"Options: trades={s2['trades']}  winrate={f(s2)}  avgPnL={s2['avg_pnl']}",
        f"Futures: trades={s3['trades']}  winrate={f(s3)}  avgPnL={s3['avg_pnl']}",
    ]
    txt = "\n".join(lines)
    open("reports/eod_report.txt","w").write(txt)
    open("reports/eod_report.html","w").write("<html><body><pre>"+txt+"</pre></body></html>")
    return {"txt":"reports/eod_report.txt","html":"reports/eod_report.html"}
