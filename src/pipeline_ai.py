# src/pipeline_ai.py
from __future__ import annotations
import os, json, datetime as dt
import pandas as pd

from config import CONFIG
from model_selector import choose_and_predict_full
from risk_manager import apply_guardrails
from ai_policy import build_context
from atr_tuner import update_from_metrics
from error_logger import RunLogger  # comprehensive logger (captures stdout/stderr, leaks, timings)

# Telegram helpers (icons + win % per trade)
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

# -------------------- utilities --------------------

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
    Convert picks DataFrame into paper 'orders' log for downstream reporting & Telegram.
    Keeps Entry/Target/SL/proba/Reason; adds fill_price (Entry), engine, status, when_utc.
    """
    if picks is None or picks.empty:
        return picks

    d = picks.copy()
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

    d["Symbol"] = d["Symbol"].astype(str).str.upper()
    d["fill_price"] = pd.to_numeric(d["Entry"], errors="coerce")

    if "size_pct" not in d.columns:
        n = max(1, len(d))
        d["size_pct"] = round(1.0 / n, 4)

    if "mode" not in d.columns:
        d["mode"] = default_mode

    d["engine"] = engine.upper()
    d["status"] = "OPEN"
    d["when_utc"] = _utcnow()

    keep = ["when_utc","engine","mode","Symbol","Entry","fill_price","Target","SL",
            "size_pct","proba","Reason","status"]
    # keep only those that exist
    keep = [k for k in keep if k in d.columns]
    return d[keep]

# -------------------- main orchestrator --------------------

def run_auto_and_algo_sessions(top_k_auto: int | None = None,
                               top_k_algo: int | None = None) -> tuple[int, int]:
    """
    - Builds runtime context
    - Selects model (light/robust/DL via model_selector)
    - Produces AUTO (top picks) and ALGO (exploration) sets
    - Applies guardrails
    - Saves paper orders, sends Telegram with icons + win %
    - Updates ATR tuner
    - ALWAYS writes comprehensive run logs & manifest (error_logger)

    Returns: (#AUTO, #ALGO)
    """
    _ensure_dirs()
    logger = RunLogger(label="pipeline")

    auto_orders = pd.DataFrame()
    algo_orders = pd.DataFrame()
    model_tag = "unknown"
    ctx = {}

    with logger.capture_all("pipeline_run", swallow=True):

        with logger.section("context/build"):
            ctx = build_context()  # includes regime, vix, data_source flags, etc.
            logger.add_meta(context=ctx)

        with logger.section("model_select"):
            tk_auto = int(CONFIG.get("modes", {}).get("auto_top_k", 5) if top_k_auto is None else top_k_auto)
            raw_df, model_tag = choose_and_predict_full(top_k=tk_auto)

        with logger.section("guardrails/AUTO"):
            auto_df = apply_guardrails(raw_df)

        with logger.section("algo_split"):
            tk_algo = int(CONFIG.get("modes", {}).get("algo_top_k", 10) if top_k_algo is None else top_k_algo)
            algo_df = pd.DataFrame(columns=auto_df.columns)
            if raw_df is not None and not raw_df.empty:
                leftover = raw_df[~raw_df["Symbol"].isin(auto_df["Symbol"])].copy()
                if not leftover.empty:
                    leftover = leftover.sort_values("proba", ascending=False).head(max(0, tk_algo))
                    algo_df = apply_guardrails(leftover)

        with logger.section("build_orders/persist"):
            auto_orders = _paper_fill_df(auto_df, engine="AUTO", default_mode="swing")
            algo_orders = _paper_fill_df(algo_df, engine="ALGO", default_mode="intraday")

            orders_path = os.path.join(DL, "paper_trades.csv")
            if auto_orders is not None and not auto_orders.empty:
                _append_csv(orders_path, auto_orders)
            if algo_orders is not None and not algo_orders.empty:
                _append_csv(orders_path, algo_orders)

        with logger.section("telegram/send"):
            header = f"NIFTY500 ProPro — {model_tag.upper()} — {dt.datetime.utcnow().strftime('%Y-%m-%d %H:%MZ')}"
            send_recommendations(auto_df=auto_orders, algo_df=algo_orders, ai_df=None, header=header)

        with logger.section("snapshot/metrics"):
            snap = {
                "when_utc": _utcnow(),
                "model_used": model_tag,
                "auto_count": 0 if auto_orders is None else int(len(auto_orders)),
                "algo_count": 0 if algo_orders is None else int(len(algo_orders)),
                "ctx": ctx
            }
            _log_json(os.path.join(MET_DIR, "last_run.json"), snap)
            logger.add_meta(model_used=model_tag, auto_count=snap["auto_count"], algo_count=snap["algo_count"])

        with logger.section("atr_tuner/update"):
            update_from_metrics(ctx)

    log_path = logger.dump()  # writes reports/logs + metrics/manifest, rotates, prunes paper_trades
    print(f"[pipeline] log written → {log_path}")

    a = 0 if auto_orders is None else int(len(auto_orders))
    b = 0 if algo_orders is None else int(len(algo_orders))
    return a, b

if __name__ == "__main__":
    a, b = run_auto_and_algo_sessions()
    print(f"AUTO: {a}, ALGO: {b}")
