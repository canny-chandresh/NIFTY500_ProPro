# src/pipeline_ai.py
from __future__ import annotations
import os, json, datetime as dt
import pandas as pd

from config import CONFIG
from model_selector import choose_and_predict_full
from risk_manager import apply_guardrails
from ai_policy import build_context
from atr_tuner import update_from_metrics

# Telegram helpers (rich formatting with icons + win%)
try:
    from telegram import send_recommendations
except Exception:
    def send_recommendations(auto_df=None, algo_df=None, ai_df=None, header=None, parse_mode="Markdown"):
        print("[TELEGRAM Fallback] (no telegram module)")
        if header: print(header)
        for name, df in [("AUTO", auto_df), ("ALGO", algo_df), ("AI", ai_df)]:
            if df is None or df.empty:
                print(f"{name}: (no trades)")
            else:
                print(f"{name}:")
                print(df.to_string(index=False))

DL = "datalake"
REP_DIR = "reports"
MET_DIR = os.path.join(REP_DIR, "metrics")

# -------------------- small utils --------------------

def _ensure_dirs():
    os.makedirs(DL, exist_ok=True)
    os.makedirs(REP_DIR, exist_ok=True)
    os.makedirs(MET_DIR, exist_ok=True)

def _utcnow():
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def _log_json(path: str, data):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w") as f:
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
    Convert a picks DataFrame into a paper 'orders' log.
    Keeps Entry/Target/SL/proba/Reason for Telegram formatting.
    Adds fill_price (same as Entry for paper), engine, status, timestamp.
    """
    if picks is None or picks.empty:
        return picks

    d = picks.copy()
    # Normalize required columns
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
        # even sizing fallback if AI policy didn’t set it
        n = max(1, len(d))
        d["size_pct"] = round(1.0 / n, 4)

    if "mode" not in d.columns:
        d["mode"] = default_mode

    d["engine"] = engine.upper()
    d["status"] = "OPEN"
    d["when_utc"] = _utcnow()

    keep = ["when_utc","engine","mode","Symbol","Entry","fill_price","Target","SL","size_pct","proba","Reason","status"]
    return d[keep]

# -------------------- main orchestrator --------------------

def run_auto_and_algo_sessions(top_k_auto: int | None = None,
                               top_k_algo: int | None = None) -> tuple[int, int]:
    """
    - Uses model_selector to get best model picks (DL/Robust/Light via AI ensemble)
    - Applies guardrails (exposure, hygiene)
    - Splits into:
        * AUTO: top picks
        * ALGO: exploration subset from remaining
    - Logs paper orders, sends Telegram with icons + win%
    - Updates ATR tuner using latest rolling metrics
    Returns: (#AUTO, #ALGO)
    """
    _ensure_dirs()
    ctx = build_context()

    # 1) Get model-selected picks (already AI-policy processed inside model_selector)
    tk_auto = int(CONFIG.get("modes", {}).get("auto_top_k", 5) if top_k_auto is None else top_k_auto)
    raw_df, model_tag = choose_and_predict_full(top_k=tk_auto)

    # Guardrails (idempotent)
    auto_df = apply_guardrails(raw_df)

    # 2) ALGO exploration: take next-best from the original raw (if any remained)
    tk_algo = int(CONFIG.get("modes", {}).get("algo_top_k", 10) if top_k_algo is None else top_k_algo)
    algo_df = pd.DataFrame(columns=auto_df.columns)
    if raw_df is not None and not raw_df.empty:
        leftover = raw_df[~raw_df["Symbol"].isin(auto_df["Symbol"])].copy()
        if not leftover.empty:
            leftover = leftover.sort_values("proba", ascending=False).head(max(0, tk_algo))
            algo_df = apply_guardrails(leftover)

    # 3) Build paper orders logs
    auto_orders = _paper_fill_df(auto_df, engine="AUTO", default_mode="swing")
    # Heuristic to tag ALGO as 'intraday' unless already tagged
    algo_mode = "intraday" if ("mode" not in algo_df.columns or algo_df["mode"].isna().all()) else None
    algo_orders = _paper_fill_df(algo_df, engine="ALGO", default_mode=(algo_mode or "intraday"))

    # 4) Persist paper orders
    orders_path = os.path.join(DL, "paper_trades.csv")
    if auto_orders is not None and not auto_orders.empty:
        _append_csv(orders_path, auto_orders)
    if algo_orders is not None and not algo_orders.empty:
        _append_csv(orders_path, algo_orders)

    # 5) Telegram message (AUTO + ALGO, with icons + probability%)
    try:
        send_recommendations(
            auto_df=auto_orders,
            algo_df=algo_orders,
            ai_df=None,
            header=f"NIFTY500 ProPro — {model_tag.upper()} — {dt.datetime.utcnow().strftime('%Y-%m-%d %H:%MZ')}"
        )
    except Exception as e:
        print("Telegram send error:", e)

    # 6) Snapshot for debugging
    snap = {
        "when_utc": _utcnow(),
        "model_used": model_tag,
        "auto_count": 0 if auto_orders is None else int(len(auto_orders)),
        "algo_count": 0 if algo_orders is None else int(len(algo_orders)),
        "ctx": ctx
    }
    _log_json(os.path.join(MET_DIR, "last_run.json"), snap)

    # 7) Let ATR tuner learn from the latest rolling metrics (gentle nudges within bounds)
    try:
        update_from_metrics(ctx)
    except Exception as e:
        print("ATR tuner update error:", e)

    return snap["auto_count"], snap["algo_count"]


if __name__ == "__main__":
    a, b = run_auto_and_algo_sessions()
    print(f"AUTO orders: {a}, ALGO orders: {b}")
