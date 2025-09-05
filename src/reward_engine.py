from __future__ import annotations
import math

def reward_from_stats(stats: dict) -> float:
    """
    Turn recent performance into a scalar reward in [0,1].
    Components:
      - win_rate (primary)
      - pnl (downside-robust, squashed, sample-size aware)
      - penalties for max drawdown & return volatility
    """
    if not stats or stats.get("trades", 0) == 0:
        return 0.0

    wr   = float(stats.get("win_rate", 0.0))           # 0..1
    pnl  = float(stats.get("pnl", 0.0))
    n    = float(stats.get("trades", 1))
    dd   = float(stats.get("max_drawdown", 0.0))       # positive number (e.g., 0.08 for -8%)
    vol  = float(stats.get("ret_vol", 0.0))            # std dev of per-trade returns

    # Smooth pnl (less sensitive with small n), clip via tanh
    pnl_adj = math.tanh(pnl / max(1.0, math.sqrt(n)))

    # Combine
    score = 0.55 * wr + 0.25 * pnl_adj - 0.10 * dd - 0.10 * vol

    # squash to [0,1] with a soft margin around 0.5
    return max(0.0, min(1.0, 0.5 + 0.5 * math.tanh(2.0 * (score - 0.5))))
