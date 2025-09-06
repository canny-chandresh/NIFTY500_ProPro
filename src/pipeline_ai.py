# src/pipeline_ai.py
from __future__ import annotations
import os, json, datetime as dt
import pandas as pd

from config import CONFIG
from error_logger import RunLogger
from config_guard import config_diff
from ai_policy import build_context
from model_selector import choose_and_predict_full
from risk_manager import apply_guardrails
from atr_tuner import update_from_metrics
from portfolio import optimize_weights
from validator import validate_orders_df
from news import fetch_and_update

# Telegram helpers
try:
    from telegram import send_recommendations
except Exception:
    def send_recommendations(auto_df=None, algo_df=None, ai_df=None, header=None, parse_mode="Markdown"):
        print("[TELEGRAM Fallback]")
        if header: print(header)
        for name, df in [("AUTO", auto_df), ("ALGO", algo_df), ("AI", ai_df)]:
            print(f"{name}:\n{df if df is not None else '(none)'}")

DL = "datalake"
REP_DIR = "reports"
MET_DIR = os.path.join(REP_DIR, "metrics")

# -------------------- utils --------------------

def _ensure_dirs():
    os.makedirs(DL, exist_ok=True)
    os.makedirs(REP_DIR, exist_ok=True)
    os.makedirs(MET_DIR, exist_ok=True)

def _utcnow():
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def _log_json(path: str, data):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

def _append_csv(path: str, df: pd.DataFrame):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    if not os.path.exists(path):
        df.to_csv(path, index=False)
    else:
        df.to_csv(path, mode="a", index=False, header=False)

# -------------------- order building --------------------

def _paper_fill_df(picks: pd.DataFrame, engine: str, default_mode: str = "swing") -> pd.DataFrame:
    """
    Convert picks DataFrame into paper 'orders' rows for downstream reporting & Telegram.
    Requires/creates: Symbol, Entry, Target, SL, proba, size_pct, Reason, mode
    Adds: fill_price, engine, status, when_utc
    """
    if picks is None or picks.empty:
        return picks

    d = picks.copy()

    # Ensure minimal fields exist
    for col in ("Symbol", "Entry", "Target", "SL", "proba"):
        if col not in d.columns:
            if col == "Symbol":
                d["Symbol"] = ""
            elif col == "Entry":
                d["Entry"] = d.get("Close", 0.0)
            elif col == "Target":
                d["Target"] = d.get("Entry", d.get("Close", 0.0)) * 1.01
            elif col == "SL":
                d["SL"] = d.get("Entry", d.get("Close", 0.0)) * 0.99
            elif col == "proba":
                d["proba"] = 0.50

    # Default sizing if portfolio optimizer wasn't able to set it
    if "size_pct" not in d.columns or d["size_pct"].isna().all():
        n = max(1, len(d))
        d["size_pct"] = round(1.0 / n, 4)

    if "mode" not in d.columns:
        d["mode"] = default_mode

    d["Symbol"] = d["Symbol"].astype(str).str.upper()
    d["fill_price"] = pd.to_numeric(d["Entry"], errors="coerce")
    d["engine"] = engine.upper()
    d["status"] = "OPEN"
    d["when_utc"] = _utcnow()

    keep = [
        "when_utc","engine","mode","Symbol","Entry","fill_price","Target","SL",
        "size_pct","proba","Reason","status"
    ]
    keep = [k for k in keep if k in d.columns]
    return d[keep]

# -------------------- main orchestrator --------------------

def run_auto_and_algo_sessions(top_k_auto: int | None = None,
                               top_k_algo: int | None = None) -> tuple[int, int]:
    """
    - Fetch & append hourly news (deduped sentiment)
    - Build runtime context (regime/VIX/etc.)
    - Model selection → ranked picks
    - Guardrails → drop unsafe picks
    - Portfolio sizing (AUTO: inv_vol; ALGO: equal)
    - Validate schemas and price relationships
    - Persist paper orders, Telegram notify
    - Update ATR tuner
    - Full run logging/manifest via RunLogger
    Returns: (#AUTO, #ALGO)
    """
    _ensure_dirs()
    logger = RunLogger(label="pipeline")

    auto_orders = pd.DataFrame()
    algo_orders = pd.DataFrame()
    model_tag = "unknown"
    ctx = {}
    cfg_diff = {}

    with logger.capture_all("pipeline_run", swallow=True):

        # 0) Config diff snapshot (audit changes)
        with logger.section("config/diff"):
            from config import CONFIG as CFG  # ensure latest in-run
            cfg_diff = config_diff(CFG)
            print("[config_diff] changed:", list(cfg_diff.get("changed", {}).keys()))
            logger.add_meta(config_hash=cfg_diff.get("cur_hash"))

        # 1) Fetch news (lightweight + dedupe)
        with logger.section("news/fetch"):
            try:
                news_info = fetch_and_update()
                logger.add_meta(news_added=news_info.get("added", 0))
                print("[news] added:", news_info)
            except Exception as e:
                print("[news] fetch error:", e)

        # 2) Build context (regime, vix, liquidity, data_source flags)
        with logger.section("context/build"):
            ctx = build_context()
            logger.add_meta(context=ctx)

        # 3) Select model and compute ranked picks
        with logger.section("model_select"):
            tk_auto = int(CONFIG.get("modes", {}).get("auto_top_k", 5) if top_k_auto is None else top_k_auto)
            raw_df, model_tag = choose_and_predict_full(top_k=tk_auto)

        # 4) Guardrails for AUTO set
        with logger.section("guardrails/AUTO"):
            auto_df = apply_guardrails(raw_df)

        # 5) ALGO exploration set (leftovers)
        with logger.section("algo_split"):
            tk_algo = int(CONFIG.get("modes", {}).get("algo_top_k", 10) if top_k_algo is None else top_k_algo)
            algo_df = pd.DataFrame(columns=auto_df.columns)
            if raw_df is not None and not raw_df.empty:
                leftover = raw_df[~raw_df["Symbol"].isin(auto_df["Symbol"])].copy()
                if not leftover.empty:
                    leftover = leftover.sort_values("proba", ascending=False).head(max(0, tk_algo))
                    algo_df = apply_guardrails(leftover)

        # 6) Portfolio sizing
        with logger.section("portfolio/size"):
            try:
                auto_df = optimize_weights(
                    auto_df, method="inv_vol",
                    max_total_risk=float(CONFIG.get("sizing",{}).get("auto_total_risk", 1.0)),
                    max_per_name=float(CONFIG.get("sizing",{}).get("auto_per_name_cap", 0.25)),
                )
            except Exception as e:
                print("[portfolio] AUTO sizing fallback:", e)

            try:
                algo_df = optimize_weights(
                    algo_df, method="equal",
                    max_total_risk=float(CONFIG.get("sizing",{}).get("algo_total_risk", 0.5)),
                    max_per_name=float(CONFIG.get("sizing",{}).get("algo_per_name_cap", 0.20)),
                )
            except Exception as e:
                print("[portfolio] ALGO sizing fallback:", e)

        # 7) Build orders & validate
        with logger.section("build_orders/persist"):
            auto_orders = _paper_fill_df(auto_df, engine="AUTO", default_mode="swing")
            algo_orders = _paper_fill_df(algo_df, engine="ALGO", default_mode="intraday")

            # Validation (non-fatal but logged)
            okA, probA = validate_orders_df(auto_orders) if auto_orders is not None and not auto_orders.empty else (True, [])
            okB, probB = validate_orders_df(algo_orders) if algo_orders is not None and not algo_orders.empty else (True, [])
            if not okA: print("[validator] AUTO issues:", probA)
            if not okB: print("[validator] ALGO issues:", probB)

            orders_path = os.path.join(DL, "paper_trades.csv")
            if auto_orders is not None and not auto_orders.empty:
                _append_csv(orders_path, auto_orders)
            if algo_orders is not None and not algo_orders.empty:
                _append_csv(orders_path, algo_orders)

        # 8) Telegram notify
        with logger.section("telegram/send"):
            header = f"NIFTY500 ProPro — {model_tag.upper()} — {dt.datetime.utcnow().strftime('%Y-%m-%d %H:%MZ')}"
            send_recommendations(auto_df=auto_orders, algo_df=algo_orders, ai_df=None, header=header)

        # 9) Snapshot small metrics
        with logger.section("snapshot/metrics"):
            snap = {
                "when_utc": _utcnow(),
                "model_used": model_tag,
                "auto_count": 0 if auto_orders is None else int(len(auto_orders)),
                "algo_count": 0 if algo_orders is None else int(len(algo_orders)),
                "ctx": ctx,
                "config_changed_keys": list(cfg_diff.get("changed", {}).keys())
            }
            _log_json(os.path.join(MET_DIR, "last_run.json"), snap)
            logger.add_meta(model_used=model_tag, auto_count=snap["auto_count"], algo_count=snap["algo_count"])

        # 10) ATR tuner update
        with logger.section("atr_tuner/update"):
            update_from_metrics(ctx)

    # Write logs, rotate, prune, etc.
    log_path = logger.dump()
    print(f"[pipeline] log written → {log_path}")

    a = 0 if auto_orders is None else int(len(auto_orders))
    b = 0 if algo_orders is None else int(len(algo_orders))
    return a, b

if __name__ == "__main__":
    a, b = run_auto_and_algo_sessions()
    print(f"AUTO: {a}, ALGO: {b}")
