from __future__ import annotations
import os, json

def _read(p, default=None):
    try:
        if os.path.exists(p):
            return json.load(open(p))
    except Exception:
        pass
    return default

def print_status():
    st  = _read("reports/metrics/ai_ensemble_state.json", {})
    pol = _read("reports/metrics/ai_policy_log.json", [])
    flg = _read("reports/metrics/algo_live_flag.json", {})
    met = _read("reports/metrics/rolling_metrics.json", {})

    print("=== AI ENSEMBLE ===")
    print(json.dumps({"weights": st.get("weights"), "last_history": (st.get("history") or [])[-3:]}, indent=2))

    print("=== LAST POLICY ===")
    print(json.dumps(pol[-1] if pol else {}, indent=2))

    print("=== ALGO LIVE FLAG ===")
    print(json.dumps(flg or {}, indent=2))

    print("=== METRICS (rolling) ===")
    print(json.dumps(met or {}, indent=2))

if __name__ == "__main__":
    print_status()
