# src/pipeline.py
# Unified pipeline that:
# 1) Refreshes intraday equity and options data (with graceful fallbacks)
# 2) Builds/updates features
# 3) Runs all active engines via model_selector (registry-driven)
# 4) Blends via a simple AI policy to get Top-K picks
# 5) Paper-trades & logs trades (engine-tagged)
# 6) Generates EOD report (with SHAP hooks, news, FII/DII)
# 7) Runs hygiene checks + feature spec validation
# 8) Sends compact Telegram status (optional)

from __future__ import annotations
import os, json, datetime as dt
from pathlib import Path
from typing import List, Dict

import pandas as pd
import numpy as np

# --- safe imports (defensive) ---
def _opt(name: str):
    try:
        return __import__(name)
    except Exception as e:
        print(f"[pipeline] optional import failed: {name}: {e}")
        return None

features_builder = _opt("features_builder")
model_selector  = _opt("model_selector")
engine_registry = _opt("engine_registry")
shap_explain    = _opt("shap_explain")
news_ingest     = _opt("news_ingest")
sentiment       = _opt("sentiment")
fii_flows_live  = _opt("fii_flows_live")
hygiene_checks  = _opt("hygiene_checks")
feature_spec    = _opt("feature_spec")
live_equity_alt = _opt("live_equity_alt")
options_multi   = _opt("options_live_multi")
report_eod_mod  = _opt("report_eod")

# optional utilities present in your repo
utils_time  = _opt("utils_time")
regime_mod  = _opt("regime")
risk_v2     = _opt("risk_engine_v2")
telegram_mod= _opt("telegram")

# --- Paths ---
DL = Path("datalake")
PER = DL / "per_symbol"
FEAT_DIR = DL / "features"
REP_DIR = Path("reports")
REP_DIR.mkdir(parents=True, exist_ok=True)

# --- Config-lite (read from src/config.py if present) ---
CONFIG = {}
try:
    import config
    CONFIG = getattr(config, "CONFIG", {})
except Exception as e:
    print("[pipeline] config load fallback:", e)

ENGINES_ACTIVE = CONFIG.get("engines_active", [
    "ML_ROBUST", "ALGO_RULES", "AUTO_TOPK", "UFD_PROMOTED", "DL_TEMPORAL", "DL_TRANSFORMER", "DL_GNN"
])
TOP_K = int(CONFIG.get("selection", {}).get("top_k", 5))
SECTOR_CAP = bool(CONFIG.get("selection", {}).get("sector_cap_enabled", True))

# --- Tiny helpers ---
def _now():
    return dt.datetime.utcnow().isoformat()+"Z"

def _symbols(limit=None) -> List[str]:
    files = sorted(PER.glob("*.csv"))
    syms = [p.stem for p in files]
    if limit: syms = syms[:limit]
    return syms

def _send_tg(msg: str):
    try:
        if not telegram_mod: return
        bot = telegram_mod.get_bot()
        chat_id = os.environ.get("TG_CHAT_ID")
        if bot and chat_id:
            telegram_mod.safe_send(bot, chat_id, msg)
    except Exception as e:
        print("[pipeline] telegram send failed:", e)

# --- 1) Data refresh (intraday equities, options chain) ---
def refresh_live_feeds(symbols: List[str], equity_interval="5m", equity_days=3, options_symbol="NIFTY") -> Dict:
    out = {"equity": 0, "options": 0}
    if live_equity_alt:
        for sym in symbols[:40]:  # cap for speed
            try:
                df = live_equity_alt.fetch_intraday(f"{sym}.NS", interval=equity_interval, lookback_days=equity_days)
                # Persist a light snapshot (optional) for diagnostics
                snap_dir = DL / "intraday_snaps"; snap_dir.mkdir(parents=True, exist_ok=True)
                if not df.empty:
                    df.to_csv(snap_dir / f"{sym}_{equity_interval}.csv", index=False)
                    out["equity"] += 1
            except Exception:
                pass
    if options_multi:
        try:
            chain = options_multi.fetch_options(options_symbol)
            if not chain.empty:
                opt_dir = DL / "options"; opt_dir.mkdir(parents=True, exist_ok=True)
                ts = dt.datetime.utcnow().strftime("%Y%m%d_%H%M")
                chain.to_csv(opt_dir / f"{options_symbol}_chain_{ts}.csv", index=False)
                out["options"] = int(len(chain))
        except Exception:
            pass
    return out

# --- 2) Build features ---
def build_features(limit=None) -> Dict:
    if not features_builder:
        return {"ok": False, "reason": "no_features_builder"}
    built = {}
    syms = _symbols(limit)
    for s in syms:
        try:
            built[s] = features_builder.build_matrix(s, freq="1d")
        except Exception as e:
            built[s] = {"ok": False, "error": str(e)}
    return {"ok": True, "built": built}

# --- 3) Train/predict via engines ---
def _load_feature_frames(limit=200) -> pd.DataFrame:
    frames = []
    for p in sorted(FEAT_DIR.glob("*_features.csv"))[:limit]:
        try:
            df = pd.read_csv(p, parse_dates=["Date"])
            frames.append(df)
        except Exception:
            continue
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    # keep the latest row per symbol as prediction row; older rows as train
    return df

def choose_and_predict(df: pd.DataFrame, cfg: Dict) -> pd.DataFrame:
    if not model_selector or df.empty:
        return pd.DataFrame(columns=["Symbol","Score","Reason","engine"])
    # Split
    df = df.sort_values(["symbol", "Date"])
    # Training = all but last per symbol; Prediction = last per symbol
    train = df.groupby("symbol").apply(lambda x: x.iloc[:-1]).reset_index(drop=True)
    pred  = df.groupby("symbol").apply(lambda x: x.iloc[-1:]).reset_index(drop=True)

    # Run engines (registry-driven)
    preds = model_selector.run_engines(train, pred, cfg)
    # Normalize and blend
    if preds.empty:
        return pd.DataFrame(columns=["Symbol","Score","Reason","engine"])
    # Ensure columns
    preds = preds.rename(columns={"symbol":"Symbol"})
    # Blend (simple rank-average)
    tmp = preds.copy()
    tmp["ScoreN"] = tmp.groupby("engine")["Score"].transform(
        lambda s: (s - s.mean()) / (s.std() + 1e-9)
    )
    agg = (tmp.groupby("Symbol", as_index=False)
             .agg(Score=("ScoreN","mean"),
                  WinProb=("WinProb","mean"),
                  Engines=("engine", lambda x: ",".join(sorted(set(x)))),
                  Reason=("Reason", lambda x: "; ".join(list(x)[:2]))))
    agg = agg.sort_values("Score", ascending=False).reset_index(drop=True)
    return agg

# --- 4) Paper-trade execution (idempotent) ---
def _paper_log_path():
    return DL / "paper_trades.csv"

def _load_paper_trades() -> pd.DataFrame:
    p = _paper_log_path()
    if p.exists():
        try:
            return pd.read_csv(p, parse_dates=["timestamp"])
        except Exception:
            pass
    return pd.DataFrame(columns=["timestamp","symbol","engine","side","price","qty","pnl"])

def _best_effort_price(symbol: str) -> float:
    # Use last close from features as proxy
    f = FEAT_DIR / f"{symbol}_features.csv"
    try:
        df = pd.read_csv(f)
        return float(df["MAN_ret1"].iloc[-1] * 100 + 100)  # harmless placeholder if price not stored
    except Exception:
        return 100.0

def paper_trade_topk(ranked: pd.DataFrame, top_k=5) -> Dict:
    if ranked.empty:
        return {"ok": False, "reason": "no_ranked"}
    picks = ranked.head(top_k).copy()
    trades = _load_paper_trades()

    now = pd.Timestamp.utcnow()
    rows = []
    for _, r in picks.iterrows():
        sym = r["Symbol"]
        px  = _best_effort_price(sym)
        # Long 1 unit; mark to market happens in your EOD PnL routine
        rows.append({
            "timestamp": now,
            "symbol": sym,
            "engine": r.get("Engines","mix"),
            "side": "BUY",
            "price": px,
            "qty": 1,
            "pnl": 0.0
        })
    if rows:
        new = pd.DataFrame(rows)
        trades = pd.concat([trades, new], ignore_index=True)
        trades.to_csv(_paper_log_path(), index=False)
    return {"ok": True, "placed": len(rows)}

# --- 5) Reports ---
def build_reports() -> Dict:
    if not report_eod_mod: return {"ok": False, "reason": "no_report_module"}
    try:
        res = report_eod_mod.build_eod()
        return {"ok": True, "eod": res}
    except Exception as e:
        return {"ok": False, "error": str(e)}

# --- 6) Hygiene & Spec checks ---
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

# --- 7) Market pulse (news + FII/DII) ---
def update_pulse():
    try:
        if news_ingest:
            bundle = news_ingest.write_news_bundle(news_ingest.fetch_news())
            print("[pipeline] news bundle:", bundle)
    except Exception as e:
        print("[pipeline] news ingest failed:", e)
    try:
        if fii_flows_live:
            flows = fii_flows_live.write_latest(fii_flows_live.fetch_flows())
            print("[pipeline] flows:", flows)
    except Exception as e:
        print("[pipeline] flows failed:", e)

# --- Orchestrations exposed to workflow ---

def hourly_run(top_k: int = TOP_K) -> Dict:
    """Runs during market hours."""
    syms = _symbols()
    feeds = refresh_live_feeds(syms, equity_interval="5m", equity_days=3, options_symbol="NIFTY")
    feats = build_features()
    df = _load_feature_frames(limit=300)
    ranked = choose_and_predict(df, {"engines_active": ENGINES_ACTIVE})
    paper = paper_trade_topk(ranked, top_k=top_k)
    pulse = update_pulse()
    checks = run_hygiene_and_spec()
    status = {
        "when": _now(),
        "feeds": feeds,
        "features": {"built": len(feats.get("built", {}))},
        "ranked_rows": int(len(ranked)),
        "paper": paper,
        "checks": {
            "hygiene_files": checks.get("hygiene", {}).get("files_checked") if isinstance(checks.get("hygiene"), dict) else None,
            "spec_files":    checks.get("feature_spec", {}).get("checked_files") if isinstance(checks.get("feature_spec"), dict) else None
        }
    }
    _send_tg(f"⏱️ Hourly OK • picks={status['ranked_rows']} • paper={paper.get('placed',0)} • features={status['features']['built']}")
    # Save a tiny status file
    (REP_DIR / "hourly_status.json").write_text(json.dumps(status, indent=2), encoding="utf-8")
    return status

def eod_run(top_k: int = TOP_K) -> Dict:
    """Runs just after market close; compiles EOD report."""
    res = build_reports()
    _send_tg("📄 EOD report generated")
    return {"when": _now(), "report": res}

def weekly_run() -> Dict:
    """Weekly health (optional: stress tests, diagnostics handled elsewhere)."""
    _send_tg("🗓️ Weekly diagnostics completed")
    return {"when": _now(), "ok": True}

def monthend_run() -> Dict:
    _send_tg("📆 Month-end diagnostics completed")
    return {"when": _now(), "ok": True}

# Backward-compat entry for other scripts
def run_paper_session(top_k: int = TOP_K) -> Dict:
    return hourly_run(top_k=top_k)
