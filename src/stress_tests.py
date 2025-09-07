"""
Replay stress scenarios from datalake.
"""

import pandas as pd
from pathlib import Path

def run_stress(symbol="NIFTY"):
    path = Path("datalake/stress")/f"{symbol}_covid.csv"
    if not path.exists(): return {}
    df = pd.read_csv(path)
    dd = (df["Close"].cummax()-df["Close"])/df["Close"].cummax()
    return {"max_dd":dd.max(),"len":len(df)}
