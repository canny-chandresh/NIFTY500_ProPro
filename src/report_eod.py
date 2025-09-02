
import os, pandas as pd, datetime as dt
from .options_executor import simulate_from_equity_recos

def build_eod():
    os.makedirs("reports", exist_ok=True)
    txt = f"NIFTY500 Pro Pro — EOD Report  ({dt.datetime.now():%Y-%m-%d %H:%M})\nNo data (stub)."
    open("reports/eod_report.txt","w").write(txt)
    open("reports/eod_report.html","w").write("<html><body><pre>"+txt+"</pre></body></html>")
    return {"txt":"reports/eod_report.txt","html":"reports/eod_report.html"}
