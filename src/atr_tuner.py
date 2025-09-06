# src/atr_tuner.py
"""
ATR tuner: adjusts stop-loss and target multipliers dynamically
based on rolling win-rates and Sharpe ratios.
"""

import os, json, datetime as dt
from metrics_tracker import summarize_last_n

STATE_FILE = "reports/metrics/atr_tuner_state.json"

DEFAULTS = {
    "swing": {"tp_mult": 3.0, "sl_mult": 1.5},
    "intraday": {"tp_mult": 1.5, "sl_mult": 1.0},
    "futures": {"tp_mult": 2.5, "sl_mult": 1.2},
    "options": {"tp_mult": 2.0, "sl_mult": 1.0},
}

def _load_state():
    if os.path.exists(STATE_FILE):
        try: return json.load(open(STATE_FILE))
        except Exception: pass
    return {"last_update": None, "per_mode": DEFAULTS.copy()}

def _save_state(state: dict):
    os.makedirs(os.path.dirname(STATE_FILE) or ".", exist_ok=True)
    json.dump(state, open(STATE_FILE, "w"), indent=2)

def get_multipliers(mode: str, ctx: dict):
    state = _load_state()
    mode = mode.lower()
    conf = state.get("per_mode", {}).get(mode, DEFAULTS.get(mode))
    if not conf: return None, None
    return conf.get("tp_mult"), conf.get("sl_mult")

def update_from_metrics(ctx: dict):
    state = _load_state()
    metr = summarize_last_n(days=10)
    auto_wr = float((metr.get("AUTO") or {}).get("win_rate", 0.0))

    for mode in DEFAULTS.keys():
        base = DEFAULTS[mode]
        adj = dict(base)
        if auto_wr >= 0.65:  # confident regime
            adj["tp_mult"] *= 1.05
            adj["sl_mult"] *= 0.95
        elif auto_wr <= 0.40:  # poor regime
            adj["tp_mult"] *= 0.9
            adj["sl_mult"] *= 1.1
        state["per_mode"][mode] = adj

    state["last_update"] = dt.datetime.utcnow().isoformat() + "Z"
    _save_state(state)
    return state
