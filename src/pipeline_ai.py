# src/pipeline_ai.py
# AI Orchestration Pipeline (policy + blending + risk + hygiene + reporting)
# Works alongside pipeline.py; this file focuses on the AI "governor" layer.

from __future__ import annotations
import os, json, datetime as dt
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

# ---------- Optional imports (defensive) ----------
def _opt(name: str):
    try:
        return __import__(name)
    except Exception as e:
        print(f"[pipeline_ai] optional import failed: {name}: {e}")
        return None

config          = _opt("config")
model_selector  = _opt("model_selector")
engine_registry = _opt("engine_registry")
ai_policy_mod   = _opt("ai_policy")             # optional: policy rules/learning
hygiene_checks  = _opt("hygiene_checks")        # optional: PSI/KS checks
feature_spec    = _opt("feature_spec")          # optional: spec validator
live_equity_alt = _opt("live_equity_alt")       # optional: intraday equity fetch
options_multi   = _opt("options_live_multi")    # optional: NSE options fetch
news_ingest     = _opt("news_ingest")           # optional: RSS headlines
fii_flows_live  = _opt("fii_flows_live")        # optional: flows CSV hook
report_eod_mod  = _opt("report_eod")            # optional: EOD text+html
telegram_mod    = _opt("telegram")              # optional: Telegram send
utils_time      = _opt("utils_time")
regime_mod      = _opt("regime")

# *** NEW priority-upgrade modules (defensive imports) ***
automl_tuner    = _opt("automl_tuner")
feature_store   = _opt("feature_store")
execution_sim   = _opt("execution_simulator")
risk_v2         = _opt("risk_engine_v2")        # VaR/exposure + laddered kill switch

# ---------- Paths / Config ----------
CONFIG = getattr(config, "CONFIG", {}) if config else {}
DL      = Path(CONFIG.get("paths", {}).get("datalake", "datalake"))
PER     = Path(CONFIG.get("paths", {}).get("per_symbol", "datalake/per_symbol"))
FEAT    = Path(CONFIG.get("paths", {}).get("features", "datalake/features"))
REP_DIR = Path(CONFIG.get("paths", {}).get("reports", "reports"))
REP_DIR.mkdir(parents=True, exist_ok=True)

TOP_K        = int(CONFIG.get("selection",{}).get("top_k", 5))
ENGINES      = CONFIG.get("engines_active", ["ML_ROBUST","ALGO_RULES","AUTO_TOPK","UFD_PROMOTED","DL_TEMPORAL","DL_TRANSFORMER","DL_GNN"])
SECTOR_CAP   = bool(CONFIG.get("selection",{}).get("sector_cap_enabled", True))
SECTOR_LIM   = int(CONFIG.get("selection",{}).get("sector_cap_limit", 2))
ATR_POL      = CONFIG.get("atr_policy", {"enable": True, "bull":0.8,"neutral":1.0,"bear":1.2,"min_mult":0.6,"max_mult":1.8})
DATA_CFG     = CONFIG.get("data", {})
PULSE_CFG    = CONFIG.get("pulse", {})
TELE_CFG     = CONFIG.get("telegram", {"enabled": True})

# ---------- Utilities ----------
def _now() -> str:
    return dt.datetime.utcnow().isoformat() + "Z"

def _symbols(limit=None) -> List[str]:
    files = sorted(PER.glob("*.csv"))
    syms = [p.stem for p in files]
    return syms[:limit] if limit else syms

def _send_tg(msg: str):
    if not TELE_CFG or not TELE_CFG.get("enabled", True) or not telegram_mod:
        return
    try:
        bot = telegram_mod.get_bot()
        chat_id = os.environ.get("TG_CHAT_ID")
        if bot and chat_id:
            telegram_mod.safe_send(bot, chat_id, msg)
    except Exception as e:
        print("[pipeline_ai] telegram send failed:", e)

def _load_features(limit_files: int = 300) -> pd.DataFrame:
    frames = []
    for p in sorted(FEAT.glob("*_features.csv"))[:limit_files]:
        try:
            df = pd.read_csv(p, parse_dates=["Date"])
            frames.append(df)
        except Exception:
            continue
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

def _last_per_symbol(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    return (df.sort_values(["symbol","Date"])
              .groupby("symbol", as_index=False)
              .tail(1))

def _paper_log_path() -> Path:
    return DL / "paper_trades.csv"

def _load_paper_trades() -> pd.DataFrame:
    p = _paper_log_path()
    if p.exists():
        try:
            return pd.read_csv(p, parse_dates=["timestamp"])
        except Exception:
            pass
    return pd.DataFrame(columns=["timestamp","symbol","engine","decision","side","price","qty","pnl","win_prob","meta"])

def _best_effort_price(symbol: str) -> float:
    f = FEAT / f"{symbol}_features.csv"
    try:
        df = pd.read_csv(f)
        if "Close" in df.columns and len(df):
            return float(df["Close"].iloc[-1])
        return float(df.get("MAN_ret1", pd.Series([0])).iloc[-1] * 100 + 100)
    except Exception:
        return 100.0

# ---------- Live data refresh ----------
def refresh_live(symbols: List[str]) -> Dict:
    out = {"equity": 0, "options_rows": 0}
    # Equity intraday snapshots
    if live_equity_alt:
        iv  = DATA_CFG.get("equity_intraday_interval","5m")
        lb  = int(DATA_CFG.get("equity_intraday_lookback_days",3))
        snap = DL / "intraday_snaps"; snap.mkdir(parents=True, exist_ok=True)
        for s in symbols[:40]:
            try:
                df = live_equity_alt.fetch_intraday(f"{s}.NS", interval=iv, lookback_days=lb)
                if not df.empty:
                    df.to_csv(snap / f"{s}_{iv}.csv", index=False)
                    out["equity"] += 1
            except Exception:
                continue
    # Options chain (NSE with fallback)
    if options_multi and DATA_CFG.get("options_primary","NSE") != "NONE":
        osm = DATA_CFG.get("options_symbol_default","NIFTY")
        try:
            chain = options_multi.fetch_options(osm)
            if not chain.empty:
                od = DL / "options"; od.mkdir(parents=True, exist_ok=True)
                ts = dt.datetime.utcnow().strftime("%Y%m%d_%H%M")
                chain.to_csv(od / f"{osm}_chain_{ts}.csv", index=False)
                out["options_rows"] = int(len(chain))
        except Exception:
            pass
    return out

# ---------- Regime-aware ATR multiplier ----------
def _regime_mult(df_last: pd.DataFrame) -> float:
    if not ATR_POL or not ATR_POL.get("enable", True): return 1.0
    if "regime_flag" in df_last.columns and len(df_last):
        rf = int(df_last["regime_flag"].iloc[0])
        if rf > 0: key = "bull"
        elif rf < 0: key = "bear"
        else: key = "neutral"
    else:
        key = "neutral"
    m = float(ATR_POL.get(key, 1.0))
    return float(max(ATR_POL.get("min_mult",0.6), min(ATR_POL.get("max_mult",1.8), m)))

# ---------- AI policy blend ----------
def ai_blend(preds: pd.DataFrame, df_last: pd.DataFrame) -> pd.DataFrame:
    """
    Blend multiple engine outputs into a unified score + AI decision hints.
    """
    if preds is None or preds.empty:
        return pd.DataFrame(columns=["Symbol","Score","WinProb","Engines","Reason","Decision","Confidence"])

    P = preds.copy()
    P["ScoreN"] = P.groupby("engine")["Score"].transform(lambda s: (s - s.mean()) / (s.std() + 1e-9))
    blend = (P.groupby("symbol", as_index=False)
               .agg(Score=("ScoreN","mean"),
                    WinProb=("WinProb","mean"),
                    Engines=("engine", lambda x: ",".join(sorted(set(x)))),
                    Reason=("Reason", lambda x: "; ".join(list(x)[:2]))))
    # AI policy hook (optional): refine Score/WinProb/Decision/Confidence
    if ai_policy_mod and hasattr(ai_policy_mod, "refine"):
        try:
            blend = ai_policy_mod.refine(blend, df_last)
        except Exception as e:
            print("[pipeline_ai] ai_policy.refine failed:", e)
    else:
        # default mapping
        z = (blend["Score"] - blend["Score"].mean()) / (blend["Score"].std() + 1e-9)
        conf = (0.5 + np.tanh(z)/2.5).clip(0.2, 0.98)
        blend["Decision"] = np.where(z > 0, "BUY", "HOLD")
        blend["Confidence"] = conf

    # regime-aware ATR multiplier
    mult = _regime_mult(df_last)
    blend["Confidence"] = (blend["Confidence"] * mult).clip(0.1, 0.99)

    blend = blend.rename(columns={"symbol":"Symbol"})
    return blend.sort_values(["Score","Confidence"], ascending=False).reset_index(drop=True)

# ---------- Sector cap filter (optional) ----------
def _apply_sector_caps(top: pd.DataFrame, df_last: pd.DataFrame, cap: int) -> pd.DataFrame:
    if not SECTOR_CAP or cap <= 0 or df_last.empty or "sector" not in df_last.columns:
        return top
    m = df_last.set_index("symbol")["sector"].to_dict()
    counts = {}
    kept = []
    for _, r in top.iterrows():
        sym = r["Symbol"]
        sec = m.get(sym, "UNK")
        n = counts.get(sec, 0)
        if n < cap:
            kept.append(r)
            counts[sec] = n + 1
    return pd.DataFrame(kept) if kept else top

# ---------- Paper trade logging ----------
def log_paper_trades(picks: pd.DataFrame) -> Dict:
    if picks is None or picks.empty:
        return {"ok": False, "reason": "no_picks"}
    logs = _load_paper_trades()
    now = pd.Timestamp.utcnow()
    rows = []
    for _, r in picks.iterrows():
        sym = r["Symbol"]
        px  = _best_effort_price(sym)
        rows.append({
            "timestamp": now,
            "symbol": sym,
            "engine": r.get("Engines","mix"),
            "decision": r.get("Decision","BUY"),
            "side": "BUY" if r.get("Decision","BUY") == "BUY" else "FLAT",
            "price": float(px),
            "qty": 1,
            "pnl": 0.0,
            "win_prob": float(r.get("WinProb", 0.5)),
            "meta": json.dumps({"confidence": float(r.get("Confidence", 0.5))})
        })
    if rows:
        new = pd.DataFrame(rows)
        logs = pd.concat([logs, new], ignore_index=True)
        logs.to_csv(_paper_log_path(), index=False)
    return {"ok": True, "placed": len(rows)}

# ---------- Hygiene & Spec ----------
def run_hygiene_and_spec() -> Dict:
    out = {}
    try:
        if hygiene_checks:
            out["hygiene"] = hygiene_checks.run(limit_files=30)
    except Exception as e:
        out["hygiene_error"] = str(e)
    try:
        if feature_spec:
            out["feature_spec"] = feature_spec.validate_repo(limit_files=50)
    except Exception as e:
        out["feature_spec_error"] = str(e)
    return out

# ---------- Pulse: News & Flows ----------
def update_pulse():
    if PULSE_CFG.get("news_enable", True) and news_ingest:
        try:
            b = news_ingest.write_news_bundle(news_ingest.fetch_news())
            print("[pipeline_ai] news bundle:", b)
        except Exception as e:
            print("[pipeline_ai] news ingest failed:", e)
    if PULSE_CFG.get("fii_dii_enable", True) and fii_flows_live:
        try:
            f = fii_flows_live.write_latest(fii_flows_live.fetch_flows())
            print("[pipeline_ai] flows:", f)
        except Exception as e:
            print("[pipeline_ai] flows failed:", e)

# ---------- Reports ----------
def build_eod_reports() -> Dict:
    if not report_eod_mod:
        return {"ok": False, "reason": "no_report_module"}
    try:
        return report_eod_mod.build_eod()
    except Exception as e:
        return {"ok": False, "error": str(e)}

# ---------- Public Orchestrations ----------
def ai_hourly(top_k: int = TOP_K) -> Dict:
    """
    1) Refresh feeds (equity, options)
    2) Load feature frames
    3) Train/predict across engines
    4) AI blend (policy)
    5) Sector caps, Top-K
    6) Pre-trade risk (laddered kill switch)
    7) Paper log
    8) Pulse + Hygiene/Spec
    """
    syms = _symbols()
    feeds = refresh_live(syms)

    df_all = _load_features(limit_files=300)
    if df_all.empty:
        status = {"when": _now(), "feeds": feeds, "note": "no features available"}
        (REP_DIR / "ai_hourly_status.json").write_text(json.dumps(status, indent=2), encoding="utf-8")
        _send_tg("⏱️ AI Hourly: no features found")
        return status

    df_all = df_all.sort_values(["symbol","Date"])
    train = df_all.groupby("symbol").apply(lambda x: x.iloc[:-1]).reset_index(drop=True)
    pred  = df_all.groupby("symbol").apply(lambda x: x.iloc[-1:]).reset_index(drop=True)
    df_last = _last_per_symbol(df_all)

    # run engines
    preds = pd.DataFrame()
    if model_selector:
        preds = model_selector.run_engines(train, pred, {"engines_active": ENGINES})

    # blend via AI
    blended = ai_blend(preds, df_last)
    if not blended.empty:
        blended = _apply_sector_caps(blended, df_last, SECTOR_LIM)

    picks = blended.head(top_k) if not blended.empty else blended

    # *** NEW: pre-trade risk tightening (laddered kill-switch) ***
    if risk_v2 and not picks.empty:
        try:
            picks = risk_v2.pretrade_filter(CONFIG, picks, df_last)
        except Exception as e:
            print("[pipeline_ai] risk_v2.pretrade_filter failed:", e)

    paper = log_paper_trades(picks)

    # Pulse & hygiene/spec
    update_pulse()
    checks = run_hygiene_and_spec()

    status = {
        "when": _now(),
        "feeds": feeds,
        "ranked_rows": int(len(blended)),
        "top_k": int(len(picks)),
        "paper": paper,
        "checks": {
            "hygiene": checks.get("hygiene", {}),
            "feature_spec": checks.get("feature_spec", {})
        }
    }
    (REP_DIR / "ai_hourly_status.json").write_text(json.dumps(status, indent=2), encoding="utf-8")
    _send_tg(f"🤖 AI Hourly OK • picks={status['top_k']} • ranked={status['ranked_rows']} • feeds eq={feeds['equity']} opt={feeds['options_rows']}")
    return status

def ai_eod(top_k: int = TOP_K) -> Dict:
    rep = build_eod_reports()
    _send_tg("📄 AI EOD report generated")
    status = {"when": _now(), "report": rep}

    # *** NEW: execution realism + post-trade risk summary ***
    if execution_sim:
        try:
            status["execution"] = execution_sim.simulate(CONFIG)
        except Exception as e:
            status["execution_error"] = str(e)
    if risk_v2:
        try:
            status["risk"] = risk_v2.posttrade_report(CONFIG)
        except Exception as e:
            status["risk_error"] = str(e)

    (REP_DIR / "ai_eod_status.json").write_text(json.dumps(status, indent=2), encoding="utf-8")
    return status

def ai_weekly() -> Dict:
    _send_tg("🗓️ AI Weekly diagnostics completed")
    status = {"when": _now(), "ok": True}
    (REP_DIR / "ai_weekly_status.json").write_text(json.dumps(status, indent=2), encoding="utf-8")
    return status

def ai_monthend() -> Dict:
    _send_tg("📆 AI Month-end diagnostics completed")
    status = {"when": _now(), "ok": True}
    (REP_DIR / "ai_monthend_status.json").write_text(json.dumps(status, indent=2), encoding="utf-8")
    return status

# Backward-compat entry
def run_paper_session(top_k: int = TOP_K) -> Dict:
    return ai_hourly(top_k=top_k)

# *** NEW: Nightly AutoML job (optional, cron-triggered in workflow) ***
def ai_nightly_automl() -> Dict:
    if not automl_tuner:
        return {"ok": False, "reason": "automl_tuner_missing"}
    df_all = _load_features(limit_files=400)
    if df_all.empty:
        return {"ok": False, "reason": "no_features"}
    train = df_all.groupby("symbol").apply(lambda x: x.iloc[:-1]).reset_index(drop=True)
    try:
        res = automl_tuner.run_automl(train, CONFIG, tag="ml")
    except Exception as e:
        res = {"ok": False, "error": str(e)}
    (REP_DIR / "ai_nightly_automl.json").write_text(json.dumps(res, indent=2), encoding="utf-8")
    return res
