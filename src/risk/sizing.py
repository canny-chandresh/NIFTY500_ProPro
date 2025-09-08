# -*- coding: utf-8 -*-
"""
risk/sizing.py
Position sizing using Kelly-lite and ATR dampening with fees & slippage.
"""

from __future__ import annotations
from config import CONFIG

def kelly_notional(prob: float, price: float, atr_pct: float) -> float:
    r = CONFIG.get("risk", {})
    max_notional = float(r.get("max_notional_per_trade", 200000.0))
    min_notional = float(r.get("min_notional_per_trade", 20000.0))
    kelly_f = float(r.get("kelly_fraction", 0.25))
    edge = (prob - 0.5)
    size = (1.0 + edge * 2.0) * max_notional * kelly_f
    if atr_pct and atr_pct > 0.04:
        size *= 0.7
    return max(min_notional, min(max_notional, size))
