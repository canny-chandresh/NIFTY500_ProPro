# src/ops_dashboard.py
import json, glob
from pathlib import Path
import pandas as pd
import streamlit as st

st.set_page_config(page_title="NIFTY500 ProPro Ops", layout="wide")
st.title("Operations Dashboard")

MET = Path("reports/metrics"); LOG = Path("reports/logs"); BT = Path("reports/backtests")

c1, c2 = st.columns(2)

with c1:
    st.subheader("Data SLIs")
    sli = MET / "sli_latest.json"
    if sli.exists():
        st.json(json.load(open(sli)))
    else:
        st.info("No SLI yet.")

    st.subheader("Walk-forward")
    wf = BT / "walkforward_summary.json"
    if wf.exists():
        st.json(json.load(open(wf)))
    else:
        st.info("No walk-forward summary yet.")

with c2:
    st.subheader("Latest Errors (tail)")
    errs = sorted(glob.glob("reports/logs/errors_only_*.txt"))
    if errs:
        st.code(open(errs[-1], encoding="utf-8").read()[-4000:])
    else:
        st.info("No errors file.")

st.subheader("Feature Drift (latest)")
fd = sorted(glob.glob(str(MET / "feature_drift_*.json")))
if fd:
    df = pd.DataFrame(json.load(open(fd[-1])).get("features", []))
    if not df.empty:
        st.dataframe(df)
