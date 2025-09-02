
import os, pandas as pd, datetime as dt
from .config import CONFIG

def is_market_closed_today():
    today = dt.date.today()
    if CONFIG.get("holiday",{}).get("skip_weekends", True) and today.weekday()>=5:
        return True
    path = os.path.join("datalake", CONFIG["holiday"]["calendar_csv"])
    if os.path.exists(path):
        try:
            df = pd.read_csv(path)
            col = "date" if "date" in df.columns else df.columns[0]
            ds = pd.to_datetime(df[col], errors="coerce").dt.date.dropna().unique()
            return today in set(ds)
        except Exception:
            return False
    return False
