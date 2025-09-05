from __future__ import annotations
import os, json, datetime as dt
from typing import Dict
from config import CONFIG

STATE_PATH = "reports/shadow/dl_kill_state.json"

def _read() -> Dict:
    if os.path.exists(STATE_PATH):
        try: return json.load(open(STATE_PATH))
        except Exception: pass
    return {"history": [], "suspended_until": None}

def _write(obj: Dict):
    os.makedirs(os.path.dirname(STATE_PATH) or ".", exist_ok=True)
    json.dump(obj, open(STATE_PATH,"w"), indent=2)

def update_from_eval(eval_res: Dict):
    """Call after each dl eval run."""
    st = _read()
    thr = CONFIG["dl"]["kill_switch"]
    now = dt.datetime.utcnow()

    # record
    rec = {
        "when_utc": eval_res.get("when_utc"),
        "hit_rate": eval_res.get("hit_rate"),
        "n_test":   eval_res.get("n_test"),
        "status":   eval_res.get("status","ok")
    }
    st["history"] = (st.get("history") or []) + [rec]
    st["history"] = st["history"][-(thr["window_runs"]*2):]  # keep recent

    # compute window stats
    window = st["history"][-thr["window_runs"]:]
    bad_runs = [r for r in window if (r.get("n_test",0) >= thr["min_test"]) and (r.get("hit_rate",1.0) < thr["hit_rate_floor"])]
    consec_bad = 0
    for r in reversed(window):
        if r.get("n_test",0) >= thr["min_test"] and r.get("hit_rate",1.0) < thr["hit_rate_floor"]:
            consec_bad += 1
        else:
            break

    # decide suspension
    suspended_until = st.get("suspended_until")
    if consec_bad >= thr["consec_bad"] or len(bad_runs) >= (thr["window_runs"]//2 + 1):
        until = now + dt.timedelta(hours=thr["cooloff_hours"])
        st["suspended_until"] = until.isoformat()+"Z"
    else:
        # auto-clear if past cooloff
        if suspended_until:
            try:
                t = dt.datetime.fromisoformat(suspended_until.replace("Z",""))
                if now > t:
                    st["suspended_until"] = None
            except Exception:
                st["suspended_until"] = None

    _write(st)
    return st

def status() -> Dict:
    st = _read()
    now = dt.datetime.utcnow()
    susp = st.get("suspended_until")
    active = True
    if susp:
        try:
            t = dt.datetime.fromisoformat(susp.replace("Z",""))
            if now <= t: active = False
        except Exception:
            st["suspended_until"] = None
    st["active"] = active
    return st
