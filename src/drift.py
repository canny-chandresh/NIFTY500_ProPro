# src/drift.py
import pandas as pd, numpy as np, os
from config import CONFIG, DL

def _psi(a: pd.Series, b: pd.Series, bins=10):
    q = np.linspace(0,1,bins+1)
    cuts = np.unique(np.quantile(pd.concat([a,b]).dropna(), q))
    if len(cuts) < 3: return 0.0
    ah = pd.cut(a, cuts, include_lowest=True).value_counts(normalize=True)
    bh = pd.cut(b, cuts, include_lowest=True).value_counts(normalize=True)
    z = (ah - bh) * np.log((ah + 1e-6)/(bh + 1e-6))
    return float(z.sum())

def drift_check():
    if not CONFIG["features"]["drift_alerts"]:
        return {"drift": None}
    fp = DL("daily_equity")
    if not os.path.exists(fp): fp = DL("daily_equity_csv")
    if not os.path.exists(fp): return {"drift": None}
    df = pd.read_parquet(fp) if fp.endswith(".parquet") else pd.read_csv(fp, parse_dates=["Date"])
    if df.empty: return {"drift": None}
    df["Date"] = pd.to_datetime(df["Date"])
    end = df["Date"].max()
    cur_start = end - pd.Timedelta(days=CONFIG["drift"]["cur_days"])
    ref_start = end - pd.Timedelta(days=CONFIG["drift"]["ref_days"] + CONFIG["drift"]["cur_days"])
    ref_end   = end - pd.Timedelta(days=CONFIG["drift"]["cur_days"])
    ref = df[(df["Date"]>=ref_start) & (df["Date"]<ref_end)]
    cur = df[(df["Date"]>=cur_start) & (df["Date"]<=end)]
    if ref.empty or cur.empty: return {"drift": None}

    def feat(x):
        return pd.DataFrame({"ret": x["Close"].pct_change().fillna(0),
                             "vol": x["Close"].pct_change().rolling(5).std().fillna(0)})
    curf = cur.groupby("Symbol").apply(lambda g: feat(g)).reset_index(drop=True)
    reff = ref.groupby("Symbol").apply(lambda g: feat(g)).reset_index(drop=True)
    if curf.empty or reff.empty: return {"drift": None}

    psi_ret = _psi(reff["ret"], curf["ret"])
    psi_vol = _psi(reff["vol"], curf["vol"])
    level = "ok"
    if psi_ret > CONFIG["drift"]["psi_alert"] or psi_vol > CONFIG["drift"]["psi_alert"]:
        level = "ALERT"
    elif psi_ret > CONFIG["drift"]["psi_warn"] or psi_vol > CONFIG["drift"]["psi_warn"]:
        level = "WARN"
    return {"drift": {"psi_ret": psi_ret, "psi_vol": psi_vol, "level": level}}
