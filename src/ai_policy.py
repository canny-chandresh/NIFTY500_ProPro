# src/ai_policy.py
from __future__ import annotations
import os, json, datetime as dt
import pandas as pd
from config import CONFIG

AI_LOG = "reports/metrics/ai_policy_log.json"
SUSPEND_FLAG = "reports/metrics/ai_policy_suspended.json"
ALGO_FLAG = "reports/metrics/algo_live_flag.json"

# -------------------------- helpers --------------------------

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

# -------------------- dynamic risk functions --------------------

def _risk_cfg():
    return ((CONFIG.get("risk") or {}).get("dynamic") or {})

def _mode_from_df_or_env(df: pd.DataFrame) -> str:
    # Prefer explicit column if your upstream adds it; else use RUN_PURPOSE heuristic
    if df is not None and not df.empty:
        for c in ("mode","trade_kind","trade_mode"):
            if c in df.columns:
                m = str(df[c].iloc[0]).strip().lower()
                if m in ("intraday","swing","futures","options"):
                    return m
    purpose = os.environ.get("RUN_PURPOSE","auto").strip().lower()
    # Map engine → default trade kind (tune if you want)
    return "swing" if purpose in ("auto","algo") else "swing"

def _percent_tp_sl_for_mode(mode: str) -> tuple[float,float]:
    per_mode = (_risk_cfg().get("per_mode") or {})
    prof = (per_mode.get(mode) or per_mode.get("swing") or {})
    tp = float(prof.get("tp_pct", 0.05))
    sl = float(prof.get("sl_pct", 0.025))
    return tp, sl

def _atr_mult_for_mode(mode: str) -> tuple[float|None,float|None]:
    per_mode = (_risk_cfg().get("per_mode") or {})
    prof = (per_mode.get(mode) or per_mode.get("swing") or {})
    return prof.get("tp_atr", None), prof.get("sl_atr", None)

def _apply_context_multipliers(tp: float, sl: float, ctx: dict) -> tuple[float,float]:
    vix = ctx.get("vix", None)
    regime = (ctx.get("regime") or "neutral").lower()
    cfg = _risk_cfg()

    # VIX adjust
    vx = cfg.get("vix_adjust") or {}
    lt = float(vx.get("low_thresh", 12.0))
    ht = float(vx.get("high_thresh", 18.0))
    if vix is not None:
        if vix <= lt:
            m = vx.get("low") or {}
            tp *= float(m.get("tp_mult", 1.0))
            sl *= float(m.get("sl_mult", 1.0))
        elif vix >= ht:
            m = vx.get("high") or {}
            tp *= float(m.get("tp_mult", 1.0))
            sl *= float(m.get("sl_mult", 1.0))

    # Regime adjust
    rg = (cfg.get("regime_adjust") or {}).get(regime) or {}
    tp *= float(rg.get("tp_mult", 1.0))
    sl *= float(rg.get("sl_mult", 1.0))
    return tp, sl

def _clamp(tp_pct: float, sl_pct: float) -> tuple[float,float]:
    cl = (_risk_cfg().get("clamp") or {})
    tp_min = float(cl.get("tp_min_pct", 0.008)); tp_max = float(cl.get("tp_max_pct", 0.080))
    sl_min = float(cl.get("sl_min_pct", 0.004)); sl_max = float(cl.get("sl_max_pct", 0.040))
    return (float(max(tp_min, min(tp_pct, tp_max))),
            float(max(sl_min, min(sl_pct, sl_max))))

# ----------------------- policy proper -------------------------

def _thresholds_base(ctx: dict, purpose: str) -> dict:
    """Base AI policy thresholds (probability, exposure) — independent of TP/SL calc."""
    thr = {
        "min_proba": 0.52,
        "max_picks": int(CONFIG.get("modes", {}).get("auto_top_k", 5)),
        "exposure_cap": 1.0
    }
    vix = ctx.get("vix", None)
    gift = ctx.get("gift_trend", 0.0)
    regime = (ctx.get("regime") or "neutral").lower()
    news_risk = ctx.get("news_risk", 0)

    if vix is not None:
        if vix >= 18:
            thr["min_proba"] += 0.03; thr["exposure_cap"] = 0.7
        elif vix <= 12:
            thr["min_proba"] -= 0.02

    if regime == "bull":
        thr["exposure_cap"] = min(1.0, thr["exposure_cap"] + 0.1)
    elif regime == "bear":
        thr["min_proba"] += 0.02; thr["exposure_cap"] = min(thr["exposure_cap"], 0.6)

    if gift < -0.01:
        thr["min_proba"] += 0.01; thr["exposure_cap"] = min(thr["exposure_cap"], 0.75)

    if news_risk >= 2:
        thr["min_proba"] += 0.02; thr["exposure_cap"] = min(thr["exposure_cap"], 0.65)

    # Purpose adjustments (ALGO explores smaller sizing)
    if purpose == "algo":
        thr["min_proba"] = max(0.50, thr["min_proba"] - 0.02)
        thr["max_picks"] = min(3, int(CONFIG.get("modes", {}).get("algo_top_k", 10)))
        thr["exposure_cap"] = min(thr["exposure_cap"], 0.50)

    thr["min_proba"] = float(max(0.50, min(0.65, thr["min_proba"])))
    thr["max_picks"] = int(max(1, min(8, thr["max_picks"])))
    thr["exposure_cap"] = float(max(0.3, min(1.0, thr["exposure_cap"])))
    return thr

def _recalc_tp_sl(df: pd.DataFrame, ctx: dict) -> pd.DataFrame:
    """Compute TP/SL using dynamic config (ATR preferred if available)."""
    if df is None or df.empty: return df
    dyn = (_risk_cfg() or {})
    if not bool(dyn.get("enable", True)):
        # legacy: keep the fixed values set earlier in pipeline (if any)
        return df

    # Determine trade mode (intraday/swing/futures/options)
    mode = _mode_from_df_or_env(df)
    tp_pct, sl_pct = _percent_tp_sl_for_mode(mode)
    use_atr = bool(dyn.get("use_atr", True))
    tp_atr, sl_atr = _atr_mult_for_mode(mode)

    # If ATR columns exist and use_atr is True: compute distance in price
    # Expecting df columns: Entry, ATR (or atr)
    d = df.copy()
    entry = d["Entry"].astype(float)

    # Context multipliers on percentage (applied later to ATR distance too)
    tp_pct_ctx, sl_pct_ctx = _apply_context_multipliers(tp_pct, sl_pct, ctx)

    # Try ATR distance; else fallback to % distance
    atr_col = None
    for c in ("ATR","atr","atr14","ATR14"):
        if c in d.columns:
            atr_col = c; break

    if use_atr and atr_col and tp_atr and sl_atr:
        tp_dist = d[atr_col].astype(float) * float(tp_atr)
        sl_dist = d[atr_col].astype(float) * float(sl_atr)

        # apply VIX/regime multipliers to ATR distances by scaling equivalent % move
        # convert ATR distance to % of price for clamping comparison
        tp_pct_eff = (tp_dist / entry.clip(lower=1e-9)).fillna(0.0) * tp_pct_ctx / max(1e-9, tp_pct)
        sl_pct_eff = (sl_dist / entry.clip(lower=1e-9)).fillna(0.0) * sl_pct_ctx / max(1e-9, sl_pct)

        # clamp effective pct
        tp_pct_eff, sl_pct_eff = _clamp(tp_pct_eff.clip(lower=0.0).values, sl_pct_eff.clip(lower=0.0).values) \
                                 if not isinstance(tp_pct_eff, float) else _clamp(float(tp_pct_eff), float(sl_pct_eff))

        # Rebuild distances from clamped pct
        tp_price = (entry * (1.0 + tp_pct_eff)).round(2)
        sl_price = (entry * (1.0 - sl_pct_eff)).round(2)
    else:
        # pure % mode with context multipliers
        tp_pct_ctx, sl_pct_ctx = _apply_context_multipliers(tp_pct, sl_pct, ctx)
        tp_pct_ctx, sl_pct_ctx = _clamp(tp_pct_ctx, sl_pct_ctx)
        tp_price = (entry * (1.0 + tp_pct_ctx)).round(2)
        sl_price = (entry * (1.0 - sl_pct_ctx)).round(2)

    d["Target"] = tp_price
    d["SL"] = sl_price
    return d

# ---------------- main entrypoint used by pipeline ----------------

def apply_policy(raw_df: pd.DataFrame, ctx: dict) -> pd.DataFrame:
    if raw_df is None or raw_df.empty:
        _save_log({"when_utc": ctx.get("when_utc"), "decisions": 0, "reason": "empty_raw"})
        return raw_df

    # Suspension check (kill-switch style)
    if _maybe_suspend_policy():
        out = raw_df.sort_values("proba", ascending=False).head(3).reset_index(drop=True)
        _save_log({"when_utc": ctx.get("when_utc"), "decisions": len(out), "reason": "ai_policy_suspended"})
        return out

    purpose = os.environ.get("RUN_PURPOSE", "auto").strip().lower()
    thr = _thresholds_base(ctx, purpose)

    # 1) confidence filter
    df = raw_df.copy()
    df = df[df["proba"] >= thr["min_proba"]].copy()
    if df.empty:
        _save_log({"when_utc": ctx.get("when_utc"), "decisions": 0, "policy": thr, "reason": "min_proba_filter"})
        return df

    # 2) abstain if uncertainty too high / confidence too low dispersion
    disp = float(df["proba"].std()) if len(df) > 1 else 0.0
    if disp < 0.01 and df["proba"].mean() < (thr["min_proba"] + 0.01):
        _save_log({"when_utc": ctx.get("when_utc"), "decisions": 0, "policy": thr, "reason": "abstain_low_conf"})
        return df.iloc[0:0]

    # 3) Recompute SL/TP using dynamic config (ATR or % + context)
    df = _recalc_tp_sl(df, ctx)

    # 4) Picks & sizing
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
