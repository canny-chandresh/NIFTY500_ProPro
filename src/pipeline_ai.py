# src/pipeline_ai.py
from __future__ import annotations
import os, json, datetime as dt
import pandas as pd

from config import CONFIG
from model_selector import choose_and_predict_full
from risk_manager import apply_guardrails
from ai_policy import build_context
from atr_tuner import update_from_metrics

# Optional telegram (safe import)
try:
    from telegram import send_text
except Exception:
    def send_text(msg: str):
        print("[TELEGRAM Fallback]\n" + msg)

DL = "datalake"
REP_DIR = "reports"
MET_DIR = os.path.join(REP_DIR, "metrics")

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

def _paper_fill_df(picks: pd.DataFrame, engine: str) -> pd.DataFrame:
    """Simulate immediate paper entries at 'Entry' and create an order log."""
    if picks is None or picks.empty: 
        return picks
    d = picks.copy()
    now = _utcnow()
    d["when_utc"] = now
    d["engine"] = engine.upper()  # AUTO / ALGO
    d["fill_price"] = d["Entry"].astype(float)
    d["status"] = "OPEN"
    # attach mode if missing
    if "mode" not in d.columns:
        # Map engine to default mode (swing) unless intraday was tagged earlier
        d["mode"] = "swing"
    return d[["when_utc","engine","mode","Symbol","fill_price","Target","SL","size_pct","proba","Reason","status"]]

def _telegram_block(df: pd.DataFrame, title: str) -> str:
    if df is None or df.empty:
        return f"*{title}*: (no trades)\n"
    rows = []
    for _, r in df.iterrows():
        sym = str(r["Symbol"])
        entry = float(r["fill_price"])
        tgt = float(r["Target"])
        sl = float(r["SL"])
        pr = float(r["proba"])
        mode = str(r.get("mode","swing")).lower()
        # icons: equity 📈, futures 📊, options ⚙️ (you can customize per mode)
        icon = "📈" if mode == "swing" else ("🏃" if mode=="intraday" else ("📊" if mode=="futures" else "⚙️"))
        rows.append(f"{icon} *{sym}* | {mode} | Buy {entry:.2f} | TP {tgt:.2f} | SL {sl:.2f} | p={pr:.2f}")
    return f"*{title}:* {len(rows)} picks\n" + "\n".join(rows) + "\n"

def run_auto_and_algo_sessions(top_k_auto: int|None=None, top_k_algo: int|None=None) -> tuple[int,int]:
    """
    - Chooses model (DL/Robust/Light) → gets top picks
    - Applies AI policy and risk guardrails
    - Simulates paper fills for AUTO (top picks) and ALGO (exploration subset)
    - Sends Telegram summary
    - Updates ATR tuner from rolling metrics
    Returns: (auto_count, algo_count)
    """
    _ensure_dirs()
    ctx = build_context()

    # AUTO — use model_selector decision for top picks
    tk_auto = int(CONFIG.get("modes",{}).get("auto_top_k", 5) if top_k_auto is None else top_k_auto)
    raw_auto, tag = choose_and_predict_full(top_k=tk_auto)
    auto_df = apply_guardrails(raw_auto)
    auto_orders = _paper_fill_df(auto_df, engine="AUTO")

    # ALGO — take a small exploratory set from remaining, if any
    tk_algo = int(CONFIG.get("modes",{}).get("algo_top_k", 10) if top_k_algo is None else top_k_algo)
    algo_df = pd.DataFrame(columns=auto_df.columns)
    if raw_auto is not None and not raw_auto.empty:
        leftover = raw_auto[~raw_auto["Symbol"].isin(auto_df["Symbol"])].copy()
        if not leftover.empty:
            leftover = leftover.sort_values("proba", ascending=False).head(max(0, tk_algo))
            algo_df = apply_guardrails(leftover)
    algo_orders = _paper_fill_df(algo_df, engine="ALGO")

    # Persist paper orders
    orders_path = os.path.join(DL, "paper_trades.csv")
    if auto_orders is not None and not auto_orders.empty:
        _append_csv(orders_path, auto_orders)
    if algo_orders is not None and not algo_orders.empty:
        _append_csv(orders_path, algo_orders)

    # Telegram summary
    msg = ""
    msg += _telegram_block(auto_orders, "AUTO (Top Picks)")
    msg += _telegram_block(algo_orders, "ALGO (Exploration)")
    try:
        send_text(msg.strip())
    except Exception as e:
        print("Telegram send error:", e)

    # Save run snapshot
    snap = {
        "when_utc": _utcnow(),
        "model_used": tag,
        "auto_count": 0 if auto_orders is None else int(len(auto_orders)),
        "algo_count": 0 if algo_orders is None else int(len(algo_orders)),
        "ctx": ctx
    }
    _log_json(os.path.join(MET_DIR, "last_run.json"), snap)

    # Let ATR tuner learn from latest rolling metrics
    try:
        update_from_metrics(ctx)
    except Exception as e:
        print("ATR tuner update error:", e)

    return snap["auto_count"], snap["algo_count"]
