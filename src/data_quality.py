from __future__ import annotations
import os, json
import numpy as np, pandas as pd
from config import CONFIG

DL_DIR = "datalake"; RPT = "reports"

def _save_json(path, obj):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    json.dump(obj, open(path,"w"), indent=2, default=str)

def _load(path):
    if not os.path.exists(path): return pd.DataFrame()
    if path.endswith(".parquet"):
        try: return pd.read_parquet(path)
        except Exception: pass
    return pd.read_csv(path)

def _sanitize(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    df = df.copy()
    if "Date" in df.columns: df = df.rename(columns={"Date":"Datetime"})
    df["Datetime"] = pd.to_datetime(df["Datetime"], utc=True, errors="coerce")
    df = df.dropna(subset=["Symbol","Datetime"]).sort_values(["Symbol","Datetime"])
    df = df.drop_duplicates(subset=["Symbol","Datetime"])
    # soft outlier capping
    capz = float(CONFIG["data"]["hygiene"].get("outlier_cap_z", 8.0))
    for col in ["Open","High","Low","Close","AdjClose","Volume"]:
        if col in df.columns:
            x = df[col].astype(float)
            mu, sd = x.mean(), x.std() + 1e-9
            z = (x - mu) / sd
            x = np.where(z >  capz, mu + capz*sd, x)
            x = np.where(z < -capz, mu - capz*sd, x)
            df[col] = x
    return df

def _gap_flag(df: pd.DataFrame, freq="60min") -> pd.DataFrame:
    if df.empty: return df
    out = []
    for s, g in df.groupby("Symbol"):
        g = g.sort_values("Datetime")
        if freq=="1min":
            # minute gaps (only for recent)
            exp = pd.date_range(g["Datetime"].min(), g["Datetime"].max(), freq="1min", tz="UTC")
        else:
            exp = pd.date_range(g["Datetime"].min(), g["Datetime"].max(), freq="60min", tz="UTC")
        gg = g.set_index("Datetime").reindex(exp)
        gg["Symbol"] = s
        gg.index.name = "Datetime"
        gg = gg.reset_index()
        gg["Missing"] = gg["Open"].isna().astype(int)
        # fill price forward for continuity; Volume zero
        for c in ["Open","High","Low","Close","AdjClose"]:
            gg[c] = gg[c].ffill()
        if "Volume" in gg.columns:
            gg["Volume"] = gg["Volume"].fillna(0.0)
        out.append(gg)
    return pd.concat(out, ignore_index=True)

def run_data_hygiene():
    os.makedirs(RPT, exist_ok=True)
    report = {}

    # hourly
    p_hour = os.path.join(DL_DIR,"hourly_equity.parquet")
    hour = _load(p_hour); hour = _sanitize(hour)
    if not hour.empty and CONFIG["data"]["hygiene"].get("gap_flag", True):
        hour = _gap_flag(hour, "60min")
    hour = hour.dropna(subset=["Datetime"]).sort_values(["Symbol","Datetime"])
    hour.to_parquet(p_hour, index=False)
    report["hourly_rows"] = len(hour); report["hourly_symbols"] = hour["Symbol"].nunique() if not hour.empty else 0

    # minute
    p_min = os.path.join(DL_DIR,"minute_equity.parquet")
    minute = _load(p_min); minute = _sanitize(minute)
    if not minute.empty and CONFIG["data"]["hygiene"].get("gap_flag", True):
        minute = _gap_flag(minute, "1min")
    minute = minute.dropna(subset=["Datetime"]).sort_values(["Symbol","Datetime"])
    minute.to_parquet(p_min, index=False)
    report["minute_rows"] = len(minute); report["minute_symbols"] = minute["Symbol"].nunique() if not minute.empty else 0

    _save_json(os.path.join(RPT,"data_health.json"), report)
    return report
