from __future__ import annotations
import os, json, datetime as dt
import pandas as pd
from config import CONFIG

AI_LOG = "reports/metrics/ai_policy_log.json"
SUSPEND_FLAG = "reports/metrics/ai_policy_suspended.json"

def _jload(path):
    if os.path.exists(path):
        try: return json.load(open(path))
        except Exception: pass
    return {}

def _save_log(rec: dict):
    os.makedirs(os.path.dirname(AI_LOG) or ".", exist_ok=True)
    hist = []
    if os.path.exists(AI_LOG):
        try: hist = json.load(open(AI_LOG))
        except Exception: hist = []
    hist.append(rec)
    json.dump(hist[-300:], open(AI_LOG, "w"), indent=2)

def _latest_vix():
    p = "datalake/vix_daily.parquet"
    if os.path.exists(p):
        try:
            v = pd.read_parquet(p).sort_values("Date")
            if not v.empty:
                return float(v.iloc[-1]["Close"])
        except Exception: pass
    return None

def _gift_trend():
    p = "datalake/gift_hourly.parquet"
    if os.path.exists(p):
        try:
            g = pd.read_parquet(p).sort_values("Date")
            if len(g) >= 12:
                prev = g["Close"].iloc[-12]
                return float((g["Close"].iloc[-1] - prev) / (prev + 1e-9))
        except Exception: pass
    return 0.0

def _news_risk():
    s = _jload("reports/sources_used.json")
    # If you aggregate news counts/sentiment, surface an integer risk 0..3 here.
    return int(s.get("news_risk", 0)) if isinstance(s, dict) else 0

def _regime_hint():
    r = _jload("datalake/regime.json")
    return (r.get("market", "neutral") if isinstance(r, dict) else "neutral").lower()

def build_context():
    return {
        "when_utc": dt.datetime.utcnow().isoformat()+"Z",
        "vix": _latest_vix(),
        "gift_trend": _gift_trend(),
        "news_risk": _news_risk(),
        "regime": _regime_hint(),
    }

def _maybe_suspend_policy() -> bool:
    """
    Suspend AI policy (fall back to safe defaults) if recent AUTO win-rate is too low.
    """
    try:
        from metrics_tracker import summarize_last_n
        metr = summarize_last_n(days=5)
        wr = float((metr.get("AUTO") or {}).get("win_rate", 0.0))
        suspended = wr < 0.40  # floor
        json.dump({"suspended": suspended, "wr": wr, "when_utc": dt.datetime.utcnow().isoformat()+"Z"},
                  open(SUSPEND_FLAG, "w"), indent=2)
        return suspended
    except Exception:
        return False

def _thresholds_by_context(ctx: dict) -> dict:
    thr = {
        "min_proba": 0.52,
        "sl_pct": 0.01,
        "tp_pct": 0.015,
        "max_picks": int(CONFIG.get("modes", {}).get("auto_top_k", 5)),
        "exposure_cap": 1.0
    }
    vix = ctx.get("vix", None)
    gift = ctx.get("gift_trend", 0.0)
    regime = (ctx.get("regime") or "neutral").lower()
    news_risk = ctx.get("news_risk", 0)

    if vix is not None:
        if vix >= 18:
            thr["min_proba"] += 0.03; thr["sl_pct"] = 0.012; thr["tp_pct"] = 0.018; thr["exposure_cap"] = 0.7
        elif vix <= 12:
            thr["min_proba"] -= 0.02; thr["tp_pct"] = 0.012

    if regime == "bull":
        thr["tp_pct"] += 0.003; thr["exposure_cap"] = min(1.0, thr["exposure_cap"] + 0.1)
    elif regime == "bear":
        thr["min_proba"] += 0.02; thr["sl_pct"] += 0.003; thr["exposure_cap"] = min(thr["exposure_cap"], 0.6)

    if gift < -0.01:
        thr["min_proba"] += 0.01; thr["exposure_cap"] = min(thr["exposure_cap"], 0.75)
    elif gift > 0.01:
        thr["tp_pct"] += 0.002

    if news_risk >= 2:
        thr["min_proba"] += 0.02; thr["exposure_cap"] = min(thr["exposure_cap"], 0.65)

    thr["min_proba"] = float(max(0.50, min(0.65, thr["min_proba"])))
    thr["sl_pct"]    = float(max(0.006, min(0.02, thr["sl_pct"])))
    thr["tp_pct"]    = float(max(0.008, min(0.03, thr["tp_pct"])))
    thr["max_picks"] = int(max(3, min(8, thr["max_picks"])))
    thr["exposure_cap"] = float(max(0.4, min(1.0, thr["exposure_cap"])))
    return thr

def apply_policy(raw_df: pd.DataFrame, ctx: dict) -> pd.DataFrame:
    """
    Turn normalized picks (Symbol,Entry,SL,Target,proba,Reason)
    into final decisions using thresholds/sizing.
    """
    if raw_df is None or raw_df.empty:
        _save_log({"when_utc": ctx.get("when_utc"), "decisions": 0, "reason": "empty_raw"})
        return raw_df

    # Suspension check (fallback to conservative)
    if _maybe_suspend_policy():
        out = raw_df.sort_values("proba", ascending=False).head(3).reset_index(drop=True)
        _save_log({"when_utc": ctx.get("when_utc"), "decisions": len(out), "reason": "ai_policy_suspended"})
        return out

    thr = _thresholds_by_context(ctx)
    df = raw_df.copy()

    # Min confidence
    df = df[df["proba"] >= thr["min_proba"]].copy()
    if df.empty:
        _save_log({"when_utc": ctx.get("when_utc"), "decisions": 0, "policy": thr, "reason": "min_proba_filter"})
        return df

    # Uncertainty abstention (dispersion very low & mean barely above threshold)
    disp = float(df["proba"].std()) if len(df) > 1 else 0.0
    if disp < 0.01 and df["proba"].mean() < (thr["min_proba"] + 0.01):
        _save_log({"when_utc": ctx.get("when_utc"), "decisions": 0, "policy": thr, "reason": "abstain_low_conf"})
        return df.iloc[0:0]

    # Recompute SL/TP by policy
    base = df["Entry"].astype(float)
    df["SL"]     = (base * (1.0 - thr["sl_pct"])).round(2)
    df["Target"] = (base * (1.0 + thr["tp_pct"])).round(2)

    # Picks & sizing
    df = df.sort_values("proba", ascending=False).head(thr["max_picks"]).reset_index(drop=True)
    weights = (df["proba"] - thr["min_proba"] + 1e-9)
    weights = weights / weights.sum()
    df["size_pct"] = (weights * thr["exposure_cap"]).round(4)

    _save_log({
        "when_utc": ctx.get("when_utc"),
        "policy": thr,
        "in_raw": int(len(raw_df)),
        "out_final": int(len(df)),
        "avg_proba": float(df["proba"].mean()) if not df.empty else None
    })
    return df
