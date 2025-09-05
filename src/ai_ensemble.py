from __future__ import annotations
import os, json, math, random, datetime as dt
from typing import Tuple, Dict
from metrics_tracker import summarize_last_n
from reward_engine import reward_from_stats

STATE = "reports/metrics/ai_ensemble_state.json"
DEFAULT_WEIGHTS = {"dl": 0.34, "robust": 0.33, "light": 0.33}

# Exploration strategy
STRATEGY = "ucb"  # "epsilon" or "ucb"
EPSILON  = 0.10   # used only if STRATEGY="epsilon"

def _read_state():
    if os.path.exists(STATE):
        try: return json.load(open(STATE))
        except Exception: pass
    return {"weights": DEFAULT_WEIGHTS.copy(), "history": []}

def _write_state(st):
    os.makedirs(os.path.dirname(STATE) or ".", exist_ok=True)
    json.dump(st, open(STATE, "w"), indent=2)

def _normalize(w: Dict[str, float]) -> Dict[str, float]:
    s = sum(max(1e-9, v) for v in w.values())
    return {k: float(max(1e-9, v) / s) for k, v in w.items()}

def _ucb_choice(w: Dict[str,float], hist: list) -> str:
    arms = list(w.keys())
    from collections import defaultdict
    cnt = defaultdict(int); avg = defaultdict(float)
    for h in hist or []:
        a = h.get("which"); r = float(h.get("reward", 0))
        if a in arms:
            n = cnt[a]
            cnt[a] += 1
            avg[a] = (avg[a]*n + r) / (cnt[a])
    t = max(1, sum(cnt.values()))
    ucb = {}
    for a in arms:
        mean = avg.get(a, 0.5)
        n_a  = max(1, cnt.get(a, 0))
        ucb[a] = mean + 0.8 * math.sqrt(math.log(t + 1) / n_a)
    return max(ucb, key=ucb.get)

def update_weights_from_recent(window_days=10):
    """
    Update model weights and attach the realized reward to the *last* chosen arm.
    Reward is computed from AUTO book performance.
    """
    st = _read_state()
    metr = summarize_last_n(days=window_days)
    r = reward_from_stats(metr.get("AUTO", {}))

    # Attach reward to last history item (the previous decision)
    hist = st.get("history", [])
    if hist:
        hist[-1]["reward"] = float(r)
        st["history"] = hist

    # Small gradient step on the last-used arm toward its reward
    last = hist[-1] if hist else None
    if last and "which" in last:
        which = last["which"]
        w = _normalize(st.get("weights", DEFAULT_WEIGHTS.copy()))
        w[which] = max(1e-6, w.get(which, 1/3) + 0.05 * (r - 0.5))
        st["weights"] = _normalize(w)

    st["last_metrics"] = metr
    _write_state(st)
    return st

def choose_model() -> Tuple[str, dict]:
    """
    Pick one of {'dl','robust','light'} using UCB (or epsilon-greedy if configured).
    """
    st = _read_state()
    w = _normalize(st.get("weights", DEFAULT_WEIGHTS.copy()))
    hist = st.get("history", [])

    if STRATEGY == "epsilon":
        which = random.choice(list(w.keys())) if random.random() < EPSILON else max(w, key=w.get)
    else:
        which = _ucb_choice(w, hist)

    st["history"] = (hist or []) + [{
        "when_utc": dt.datetime.utcnow().isoformat()+"Z",
        "which": which,
        "weights": w
    }]
    _write_state(st)
    return which, st
