# src/status_probe.py
"""
Utility for GitHub Actions to print live status of ML/DL/AI modules.
"""

import os, json, datetime as dt

def print_status():
    out = {"when_utc": dt.datetime.utcnow().isoformat()+"Z"}

    for fn in [
        "reports/metrics/atr_tuner_state.json",
        "reports/metrics/rolling_metrics.json",
        "reports/metrics/ai_policy_log.json",
        "reports/metrics/algo_live_flag.json",
    ]:
        if os.path.exists(fn):
            try: out[fn] = json.load(open(fn))
            except Exception: out[fn] = "unreadable"
        else:
            out[fn] = None

    print("=== STATUS SNAPSHOT ===")
    print(json.dumps(out, indent=2))
