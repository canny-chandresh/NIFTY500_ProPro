# -*- coding: utf-8 -*-
"""
ai_policy.py
Policy layer that sits above engines and shapes the final decision:
- confidence gating
- regime-aware weights (light)
- throttle/kill-switch awareness (delegates to kill_switch if present)
- optional uncertainty tempering if calibration provides it
"""

from __future__ import annotations
from typing import Dict, Any, List, Tuple
import math
from pathlib import Path

from config import CONFIG

def regime_weight_hint(regime: str) -> Dict[str, float]:
    """
    Light regime-aware weighting hints. Phase-2 keeps this simple because
    the heavy-lift ensembling is handled by meta_stacker (if available).
    """
    regime = (regime or "").lower()
    if "bear" in regime:
        return {"ml": 0.35, "boost": 0.40, "dl": 0.25}
    if "bull" in regime:
        return {"ml": 0.20, "boost": 0.45, "dl": 0.35}
    # neutral/chop
    return {"ml": 0.30, "boost": 0.40, "dl": 0.30}

def apply_confidence_gate(picks: List[Dict[str, Any]],
                          min_prob: float = 0.52) -> List[Dict[str, Any]]:
    out = [p for p in picks if float(p.get("prob_win", 0.0)) >= float(min_prob)]
    return out or picks  # never return empty list; fall back to originals

def temper_uncertainty(prob: float, sigma: float | None) -> float:
    """
    If calibration provided an uncertainty estimate (sigma), shrink toward 0.5.
    """
    if sigma is None or not (0.0 <= prob <= 1.0):
        return prob
    alpha = max(0.0, min(1.0, sigma))  # 0..1
    return 0.5 + (prob - 0.5) * (1.0 - 0.6 * alpha)

def kill_switch_ok() -> bool:
    """
    Ask kill_switch (if present) whether we can trade.
    """
    try:
        import kill_switch as ks
        return ks.can_trade(CONFIG)
    except Exception:
        return True

def finalize(picks: List[Dict[str, Any]],
             regime: str | None = None,
             min_prob: float = 0.52) -> List[Dict[str, Any]]:
    """
    Final AI step called by pipeline_ai before formatting Telegram lines.
    """
    # 1) Kill-switch
    if CONFIG.get("killswitch", {}).get("enabled", True) and not kill_switch_ok():
        # turn all picks into shadow/observation (paper only)
        for p in picks:
            p["Side"] = "OBSERVE"
            p["reason"] = (p.get("reason","") + " | KS").strip()
        return picks

    # 2) Confidence gating
    gated = apply_confidence_gate(picks, min_prob=min_prob)

    # 3) (Optional) regime hint (attach metadata; meta_stacker uses it if present)
    hints = regime_weight_hint(regime or "neutral")
    for p in gated:
        p["_ai_hints"] = hints

    return gated
