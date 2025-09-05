from __future__ import annotations
import os, pandas as pd
from config import CONFIG

def _load_csv(path: str):
    if os.path.exists(path):
        try:
            df = pd.read_csv(path)
            if "Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Date"])
                df = df.sort_values("Date")
            return df
        except Exception:
            return None
    return None

def _vix_tag():
    vdf = _load_csv("datalake/indiavix.csv")
    if vdf is None or vdf.empty: return ("UNK", None)
    v = float(vdf["VIX"].iloc[-1])
    if v >= 20.0: return ("VIX_HIGH", v)
    if v <= 13.0: return ("VIX_LOW",  v)
    return ("VIX_MED", v)

def _gift_bias():
    """Use GIFT Nifty daily % change as a weak bias (if available)."""
    gdf = _load_csv("datalake/gift_nifty.csv")
    if gdf is None or gdf.empty: return ("GIFT_UNK", None)
    if {"Close","Open"}.issubset(gdf.columns):
        last = gdf.iloc[-1]
        chg = (float(last["Close"]) - float(last["Open"])) / max(1e-6, float(last["Open"]))
        if chg >= 0.005: return ("GIFT_UP", chg)
        if chg <= -0.005: return ("GIFT_DOWN", chg)
        return ("GIFT_FLAT", chg)
    return ("GIFT_UNK", None)

def _news_risk():
    p = "reports/news_pulse.json"
    if not os.path.exists(p): return ("NEWS_UNK", 0)
    try:
        j = pd.read_json(p, typ="series")
        neg = int(j.get("hits_negative", 0))
        thr = int(CONFIG.get("news",{}).get("high_risk_threshold", 3))
        return ("NEWS_RISK_HIGH", neg) if neg >= thr else ("NEWS_RISK_LOW", neg)
    except Exception:
        return ("NEWS_UNK", 0)

def apply_regime_adjustments() -> dict:
    base = "NEUTRAL"
    tags = []

    vtag, vval = _vix_tag()
    tags.append(f"{vtag}={'' if vval is None else round(vval,2)}")
    if vtag == "VIX_HIGH" and base != "BULL": base = "BEAR"
    if vtag == "VIX_LOW"  and base != "BEAR": base = "BULL"

    gtag, gval = _gift_bias()
    tags.append(f"{gtag}={'' if gval is None else round(100*gval,2)}%")
    # nudge only
    # (you can wire this into selection rules in model_selector if you want a numeric tilt)

    ntag, nval = _news_risk()
    tags.append(f"{ntag}={nval}")

    return {"regime": base, "reason": "; ".join(tags)}
