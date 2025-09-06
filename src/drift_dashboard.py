# src/drift_dashboard.py
import json, glob
from pathlib import Path
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Feature Drift Dashboard", layout="wide")
st.title("Feature Drift")

MET = Path("reports/metrics")
files = sorted(glob.glob(str(MET / "feature_drift_*.json")))
if not files:
    st.info("No drift files yet.")
else:
    latest = files[-1]
    st.caption(f"Latest: {latest}")
    data = json.load(open(latest))
    df = pd.DataFrame(data.get("features", []))
    if df.empty:
        st.info("Empty drift payload.")
    else:
        st.dataframe(df)
        st.bar_chart(df.set_index("name")[["psi","ks"]])
