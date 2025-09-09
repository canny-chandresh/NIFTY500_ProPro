# -*- coding: utf-8 -*-
from dataclasses import dataclass
from typing import List

@dataclass
class AlphaDef:
    name: str
    module: str
    enabled: bool
    fast: bool
    shadow: bool  # start as shadow; promote via discovery or manual flip

ALPHAS: List[AlphaDef] = [
    # Existing core
    AlphaDef("alpha_gap_decay",           "alpha_gap_decay",           True,  True,  True),
    AlphaDef("alpha_pair_flow",           "alpha_pair_flow",           True,  False, True),
    AlphaDef("alpha_event_guard",         "alpha_event_guard",         True,  False, True),

    # Earlier additions
    AlphaDef("alpha_opening_gap",         "alpha_opening_gap",         True,  True,  True),
    AlphaDef("alpha_news_sentiment",      "alpha_news_sentiment",      True,  True,  True),
    AlphaDef("alpha_anomaly_flag",        "alpha_anomaly_flag",        True,  True,  True),
    AlphaDef("alpha_momentum_short",      "alpha_momentum_short",      True,  True,  True),
    AlphaDef("alpha_momentum_long",       "alpha_momentum_long",       True,  False, True),
    AlphaDef("alpha_volume_surge",        "alpha_volume_surge",        True,  True,  True),
    AlphaDef("alpha_volatility_breakout", "alpha_volatility_breakout", True,  True,  True),
    AlphaDef("alpha_ema_crossover",       "alpha_ema_crossover",       True,  True,  True),
    AlphaDef("alpha_iv_riskpremium",      "alpha_iv_riskpremium",      True,  False, True),

    # NEW “game-changers”
    AlphaDef("alpha_orb_breakout",        "alpha_orb_breakout",        True,  True,  True),
    AlphaDef("alpha_vwap_distance",       "alpha_vwap_distance",       True,  True,  True),
    AlphaDef("alpha_sector_breadth",      "alpha_sector_breadth",      True,  True,  True),
    AlphaDef("alpha_rel_strength_index",  "alpha_rel_strength_index",  True,  True,  True),
    AlphaDef("alpha_donchian_breakout",   "alpha_donchian_breakout",   True,  True,  True),
    AlphaDef("alpha_kama_trend",          "alpha_kama_trend",          True,  False, True),
    AlphaDef("alpha_bb_position",         "alpha_bb_position",         True,  True,  True),
    AlphaDef("alpha_rsi_divergence",      "alpha_rsi_divergence",      True,  False, True),
    AlphaDef("alpha_entropy_vol",         "alpha_entropy_vol",         True,  False, True),
    AlphaDef("alpha_autocorr_1d",         "alpha_autocorr_1d",         True,  False, True),
    AlphaDef("alpha_turnover_liquidity",  "alpha_turnover_liquidity",  True,  True,  True),
    AlphaDef("alpha_avwap_ytd",           "alpha_avwap_ytd",           True,  False, True),
]
