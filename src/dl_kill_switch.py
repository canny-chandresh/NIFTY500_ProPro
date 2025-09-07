# src/dl_kill_switch.py
from __future__ import annotations
import json, datetime as dt
from pathlib import Path
from typing import Dict, Any

STATE = Path("reports/metrics/dl_kill_state.json")
STATE.parent.mkdir(parents=True, exist_ok=True)

DEFAULT_CFG = {
    "enabled": True,
    "lookback_runs": 3,        # consecutive runs to evaluate
    "min_winrate_pct": 30.0,   # floor; below this -> count a failure
    "cooldown_runs": 4         # pause DL training for these many runs after trip
}

def _load_state() -> Dict[str, Any]:
    if STATE.exists():
        try: return json.loads(STATE.read_text())
        except Exception: pass
    return {"history": [], "cooldown_left": 0, "tripped": False}

def _save_state(s: Dict[str, Any]) -> None:
    s["updated_utc"] = dt.datetime.utcnow().isoformat() + "Z"
    STATE.write_text(json.dumps(s, indent=2), encoding="utf-8")

def should_train_dl(cfg: Dict[str, Any] = None) -> tuple[bool, Dict[str, Any]]:
    cfg = {**DEFAULT_CFG, **(cfg or {})}
    st = _load_state()

    # respect cooldown
    if st.get("cooldown_left", 0) > 0:
        st["cooldown_left"] = max(0, int(st["cooldown_left"]) - 1)
        _save_state(st)
        return False, {"reason": "cooldown", "cooldown_left": st["cooldown_left"]}

    # if never tripped or not enough history, allow training
    hist = st.get("history", [])[-cfg["lookback_runs"]:]
    if len(hist) < cfg["lookback_runs"]:
        return True, {"reason": "warming", "have": len(hist), "need": cfg["lookback_runs"]}

    # evaluate last N runs
    fails = sum(1 for h in hist if (h or {}).get("win_rate", 100.0) < cfg["min_winrate_pct"])
    if fails >= cfg["lookback_runs"]:
        # trip kill switch
        st["tripped"] = True
        st["cooldown_left"] = int(cfg["cooldown_runs"])
        _save_state(st)
        return False, {"reason": "tripped", "fails": fails, "cooldown_left": st["cooldown_left"]}

    return True, {"reason": "ok", "fails_in_window": fails}

def record_result(win_rate_pct: float, cfg: Dict[str, Any] = None) -> Dict[str, Any]:
    cfg = {**DEFAULT_CFG, **(cfg or {})}
    st = _load_state()
    hist = st.get("history", [])
    hist.append({"when_utc": dt.datetime.utcnow().isoformat()+"Z", "win_rate": float(win_rate_pct)})
    # keep last 20 results
    st["history"] = hist[-20:]
    # if performing well, clear trip state
    if win_rate_pct >= cfg["min_winrate_pct"]:
        st["tripped"] = False
    _save_state(st)
    return {"history_len": len(st["history"]), "tripped": st.get("tripped", False), "cooldown_left": st.get("cooldown_left", 0)}
