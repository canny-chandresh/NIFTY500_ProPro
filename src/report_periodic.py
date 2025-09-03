# src/report_periodic.py
import os, pandas as pd
from config import DL

def _safe_load(fp):
    if os.path.exists(fp):
        try: return pd.read_csv(fp)
        except Exception: pass
    return pd.DataFrame()

def _agg(df):
    if df.empty: return pd.DataFrame()
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    g = df.groupby("date").agg(trades=("symbol","count"),
                               win=("target_hit","sum"),
                               pnl=("pnl_pct","mean")).reset_index()
    g["winrate"] = g["win"]/g["trades"]
    return g

def build_period(kind="D"):
    os.makedirs("reports", exist_ok=True)
    eq = _agg(_safe_load(DL("paper_fills")))
    op = _agg(_safe_load("datalake/options_paper.csv"))
    fu = _agg(_safe_load("datalake/futures_paper.csv"))

    def _res(df, label):
        if df.empty: return f"{label}: NA"
        if kind=="W": df = df.set_index("date").resample("W").mean().reset_index()
        if kind=="M": df = df.set_index("date").resample("M").mean().reset_index()
        return f"{label}: periods={len(df)}  avg_winrate={df['winrate'].mean():.2%}  avg_pnl={df['pnl'].mean():.3f}"

    txt = "\n".join([_res(eq,"Equity"), _res(op,"Options"), _res(fu,"Futures")])
    open(f"reports/agg_report_{kind}.txt","w").write(txt)
    open(f"reports/agg_report_{kind}.html","w").write("<html><body><pre>"+txt+"</pre></body></html>")
    return {"txt": f"reports/agg_report_{kind}.txt", "html": f"reports/agg_report_{kind}.html"}
