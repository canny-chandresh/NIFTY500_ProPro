# -*- coding: utf-8 -*-
"""
pipeline_ai.py
Turns features into ranked picks. Phase-1 provides a safe baseline:
- calls model_selector engines
- crude probability mapping (until Phase-2 calibration)
- ATR-aware stops/targets and simple notional sizing
- produces human-readable lines for Telegram
"""

from __future__ import annotations
from typing import Dict, Any, List, Tuple
import numpy as np
import pandas as pd

from config import CONFIG
import model_selector as ms

# Optional imports (Phase-2 will provide these)
def _try_import(name: str):
    try:
        return __import__(name, fromlist=["*"])
    except Exception:
        return None

def _simple_prob(x: float) -> float:
    # map [0..1] blend score into a soft probability
    x = max(0.0, min(1.0, x))
    # slightly optimistic S-curve
    return 0.35 + 0.5 * x

def _size_trade(price: float, atr_pct: float, prob: float) -> float:
    r = CONFIG.get("risk", {})
    max_notional = float(r.get("max_notional_per_trade", 200000.0))
    min_notional = float(r.get("min_notional_per_trade", 20000.0))
    kelly_f = float(r.get("kelly_fraction", 0.25))

    # crude edge proxy
    edge = (prob - 0.5)
    size = max(min_notional, min(max_notional, (1.0 + edge * 2.0) * max_notional * kelly_f))
    if atr_pct and atr_pct > 0.04:  # dampen if too volatile
        size *= 0.7
    return float(size)

def score_and_select(
    ff: pd.DataFrame,
    X: np.ndarray,
    cols: List[str],
    meta: Dict[str, Any],
    top_k: int = 5
) -> List[Dict[str, Any]]:
    """
    Returns a list of picks: [{symbol, prob_win, notional, target, stop, kind, Side, reason}, ...]
    """
    symbols = ff["symbol"].tolist()

    # Engines
    s_ml    = ms.score_ml_light(X)             # always available
    s_boost = ms.score_boosters(X)             # if boosters missing -> falls back
    s_ft    = ms.score_dl_ft(X)                # deep tabular if present
    s_tcn   = ms.score_dl_tcn({"symbols": symbols})
    s_tst   = ms.score_dl_tst({"symbols": symbols})

    blend = ms.blend_scores(symbols, s_ml, s_boost, s_ft, s_tcn, s_tst)  # 0..1-ish

    rows = []
    for i, sym in enumerate(symbols):
        b = float(blend.get(sym, 0.0))
        prob = _simple_prob(b)                 # Phase-2 swaps with calibrated prob
        price = float(ff.loc[ff["symbol"]==sym, "close"].fillna(0.0).values[:1] or [0.0][0])
        atrp  = float(ff.loc[ff["symbol"]==sym, "atr_pct"].fillna(0.0).values[:1] or [0.0][0])

        notional = _size_trade(price, atrp, prob)

        # basic ATR-based TP/SL
        atr_stop_mult   = CONFIG["risk"].get("atr_stop_mult", 1.2)
        atr_target_mult = CONFIG["risk"].get("atr_target_mult", 2.0)
        stop = price * (1.0 - atrp * atr_stop_mult) if price > 0 and atrp > 0 else "-"
        tgt  = price * (1.0 + atrp * atr_target_mult) if price > 0 and atrp > 0 else "-"

        rows.append({
            "symbol": sym,
            "prob_win": prob,
            "notional": notional,
            "target": tgt,
            "stop": stop,
            "kind": "equity",
            "Side": "BUY",
            "reason": "AI blend (Phase-1)"
        })

    # rank by prob_win then notional
    rows.sort(key=lambda r: (r["prob_win"], r["notional"]), reverse=True)
    return rows[: max(1, int(top_k))]

def format_telegram_lines(picks: List[Dict[str, Any]]) -> List[str]:
    lines = []
    for p in picks:
        icon = {"equity":"📈","option":"🦾","future":"📊"}.get(p.get("kind","equity"), "📈")
        sym = p["symbol"]; side = p.get("Side","BUY")
        prob = p.get("prob_win", 0.0)
        nto  = p.get("notional", 0.0)
        tgt  = p.get("target","-"); sl = p.get("stop","-")
        lines.append(f"{icon} {sym} • {side} • p={prob:.2f} • ₹{nto:,.0f}\n🎯 {tgt} | ⛔ {sl}")
    return lines
