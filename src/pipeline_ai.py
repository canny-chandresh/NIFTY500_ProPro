from __future__ import annotations
import os
from config import CONFIG

def _run_block(purpose: str, top_k: int):
    # purpose: "auto" or "algo" (influences AI policy thresholds via env var)
    os.environ["RUN_PURPOSE"] = purpose

    from model_selector import choose_and_predict_full
    from ai_policy import build_context, decide_algo_live
    from live_router import submit

    ctx = build_context()
    picks, tag = choose_and_predict_full(top_k=top_k)
    if picks is None or picks.empty:
        return 0

    if purpose == "auto":
        live_ok = bool(CONFIG.get("live", {}).get("enable_auto_live", False)) and not bool(CONFIG.get("live", {}).get("dry_run", True))
        mode = "LIVE" if live_ok else "PAPER"
        max_trades = int(CONFIG.get("modes", {}).get("auto_top_k", 5))
        per_cap = 0.20
    else:
        flag = decide_algo_live(ctx)
        live_ok = bool(flag.get("allowed", False)) and not bool(flag.get("dry_run", True)) and bool(flag.get("enable_algo_live", False))
        mode = "LIVE" if live_ok else "PAPER"
        rules = CONFIG.get("live", {}).get("algo_live_rules", {})
        max_trades = int(rules.get("max_extra_trades", 3))
        per_cap = float(rules.get("per_trade_cap", 0.10))

    cnt = 0
    book = "AUTO" if purpose == "auto" else "ALGO"
    for _, r in picks.head(max_trades).iterrows():
        sym = str(r["Symbol"])
        entry = float(r["Entry"]); sl = float(r["SL"]); tp = float(r["Target"])
        size_pct = float(r.get("size_pct", 0.1))
        size_pct = min(size_pct, per_cap)  # safety

        # paper qty proxy; wire capital→qty later
        qty = max(1, int(1 * size_pct * 10))
        meta = {
            "Reason": str(r.get("Reason","")),
            "proba": float(r.get("proba", 0.5)),
            "size_pct": size_pct,
            "model_tag": tag,
            "purpose": purpose
        }
        submit(sym, "BUY", qty, entry, sl, tp, book=book, mode_tag=mode, meta=meta)
        cnt += 1
    return cnt

def run_auto_and_algo_sessions():
    a = _run_block("auto", top_k=int(CONFIG.get("modes",{}).get("auto_top_k",5)))
    b = _run_block("algo", top_k=int(CONFIG.get("modes",{}).get("algo_top_k",10)))
    return a, b
