from __future__ import annotations
import os, json, datetime as dt
import pandas as pd
from config import CONFIG

AI_LOG = "reports/metrics/ai_policy_log.json"
SUSPEND_FLAG = "reports/metrics/ai_policy_suspended.json"
ALGO_FLAG = "reports/metrics/algo_live_flag.json"

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
    try:
        from metrics_tracker import summarize_last_n
        metr = summarize_last_n(days=5)
        wr = float((metr.get("AUTO") or {}).get("win_rate", 0.0))
        suspended = wr < 0.40
        json.dump({"suspended": suspended, "wr": wr, "when_utc": dt.datetime.utcnow().isoformat()+"Z"},
                  open(SUSPEND_FLAG, "w"), indent=2)
        return suspended
    except Exception:
        return False

def _thresholds_by_context(ctx: dict, purpose: str) -> dict:
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

    # Purpose adjustments (ALGO explores smaller sizing)
    if purpose == "algo":
        thr["min_proba"] = max(0.50, thr["min_proba"] - 0.02)
        thr["max_picks"] = min(3, int(CONFIG.get("modes", {}).get("algo_top_k", 10)))
        thr["exposure_cap"] = min(thr["exposure_cap"], 0.50)

    thr["min_proba"] = float(max(0.50, min(0.65, thr["min_proba"])))
    thr["sl_pct"]    = float(max(0.006, min(0.02, thr["sl_pct"])))
    thr["tp_pct"]    = float(max(0.008, min(0.03, thr["tp_pct"])))
    thr["max_picks"] = int(max(1, min(8, thr["max_picks"])))
    thr["exposure_cap"] = float(max(0.3, min(1.0, thr["exposure_cap"])))
    return thr

def apply_policy(raw_df: pd.DataFrame, ctx: dict) -> pd.DataFrame:
    if raw_df is None or raw_df.empty:
        _save_log({"when_utc": ctx.get("when_utc"), "decisions": 0, "reason": "empty_raw"})
        return raw_df

    # Suspension check
    if _maybe_suspend_policy():
        out = raw_df.sort_values("proba", ascending=False).head(3).reset_index(drop=True)
        _save_log({"when_utc": ctx.get("when_utc"), "decisions": len(out), "reason": "ai_policy_suspended"})
        return out

    purpose = os.environ.get("RUN_PURPOSE", "auto").strip().lower()
    thr = _thresholds_by_context(ctx, purpose)
    df = raw_df.copy()

    # Min confidence
    df = df[df["proba"] >= thr["min_proba"]].copy()
    if df.empty:
        _save_log({"when_utc": ctx.get("when_utc"), "decisions": 0, "policy": thr, "reason": "min_proba_filter"})
        return df

    # Uncertainty abstention
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
        "purpose": purpose,
        "policy": thr,
        "in_raw": int(len(raw_df)),
        "out_final": int(len(df)),
        "avg_proba": float(df["proba"].mean()) if not df.empty else None
    })
    return df

def decide_algo_live(ctx: dict) -> dict:
    """
    AI gatekeeper for ALGO going live. Writes reports/metrics/algo_live_flag.json
    and returns {"allowed": bool, "reason": str, ...}
    """
    live_cfg = CONFIG.get("live", {})
    rules = live_cfg.get("algo_live_rules", {})
    enable_algo_live = bool(live_cfg.get("enable_algo_live", False))
    conditional = bool(live_cfg.get("conditional_algo_live", True))
    dry_run = bool(live_cfg.get("dry_run", True))

    out = {"allowed": False, "dry_run": dry_run, "conditional": conditional, "enable_algo_live": enable_algo_live}

    if not enable_algo_live:
        out["reason"] = "enable_algo_live_false"
    elif dry_run:
        out["reason"] = "dry_run_true"
    elif not conditional:
        out["allowed"] = True
        out["reason"] = "conditional_false_allow_all"
    else:
        try:
            from metrics_tracker import summarize_last_n
            metr = summarize_last_n(days=10)
            auto_wr = float((metr.get("AUTO") or {}).get("win_rate", 0.0))
        except Exception:
            auto_wr = 0.0

        vix = ctx.get("vix") or 99.0
        regime = (ctx.get("regime") or "neutral").lower()

        if auto_wr >= float(rules.get("auto_wr_min", 0.65)) and \
           vix <= float(rules.get("vix_max", 14.0)) and \
           regime in set([r.lower() for r in rules.get("regimes_ok", ["bull","neutral"])]):
            out["allowed"] = True
            out["reason"] = "conditions_met"
        else:
            out["reason"] = f"conditions_not_met wr={auto_wr:.2f} vix={vix} regime={regime}"

    os.makedirs(os.path.dirname(ALGO_FLAG) or ".", exist_ok=True)
    json.dump({**out, "when_utc": dt.datetime.utcnow().isoformat()+"Z"}, open(ALGO_FLAG,"w"), indent=2)
    return out
