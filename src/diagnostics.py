# src/diagnostics.py
from __future__ import annotations
import os, sys, json, glob, datetime as dt, traceback
from pathlib import Path

# Make src importable when run from repo root or Actions
if Path("src").exists():
    sys.path.append("src")

OUT_DIR = Path("reports/metrics")
OUT_DIR.mkdir(parents=True, exist_ok=True)

def _utc() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat()+"Z"

def run() -> dict:
    out = {
        "when_utc": _utc(),
        "datalake_present": os.path.isdir("datalake"),
        "reports_present": os.path.isdir("reports"),
        "src_present": os.path.isdir("src"),
        "env": {
            "TG_BOT_TOKEN_set": bool(os.getenv("TG_BOT_TOKEN")),
            "TG_CHAT_ID_set": bool(os.getenv("TG_CHAT_ID")),
        },
        "imports": {},
        "feature_audit": {},
        "latest_manifest": {},
        "latest_log": None,
        "paper_trades": {
            "exists": os.path.exists("datalake/paper_trades.csv"),
            "rows": None,
            "schema_ok": None,
            "problems": []
        }
    }

    # Imports sanity
    mods = ["config","pipeline_ai","entrypoints","error_logger",
            "market_hours","locks","validator","portfolio","news",
            "backtester","archiver","config_guard","ai_policy","risk_manager",
            "model_selector","atr_tuner","metrics_tracker"]
    for m in mods:
        try:
            __import__(m)
            out["imports"][m] = "OK"
        except Exception as e:
            out["imports"][m] = f"ERROR: {e}"

    # Feature audit (if available)
    try:
        from feature_audit import audit
        out["feature_audit"] = audit()
    except Exception as e:
        out["feature_audit"] = {"error": repr(e)}

    # Latest manifest/log
    try:
        mans = sorted(glob.glob("reports/metrics/run_manifest_*.json"))
        if mans:
            out["latest_manifest"] = json.load(open(mans[-1], "r", encoding="utf-8"))
    except Exception as e:
        out["latest_manifest"] = {"error": repr(e)}

    try:
        logs = sorted(glob.glob("reports/logs/*.log"))
        if logs:
            out["latest_log"] = logs[-1]
    except Exception:
        pass

    # paper_trades quick schema check
    p = Path("datalake/paper_trades.csv")
    if p.exists():
        try:
            import pandas as pd
            df = pd.read_csv(p, nrows=200)
            out["paper_trades"]["rows"] = int(df.shape[0])
            from validator import validate_orders_df
            ok, problems = validate_orders_df(df)
            out["paper_trades"]["schema_ok"] = bool(ok)
            out["paper_trades"]["problems"] = problems
        except Exception as e:
            out["paper_trades"]["schema_ok"] = False
            out["paper_trades"]["problems"] = [repr(e)]

    # Write diagnostics snapshot
    with open(OUT_DIR / "diagnostics.json", "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)
    print(json.dumps(out, indent=2))
    return out

if __name__ == "__main__":
    try:
        run()
    except Exception:
        print("Diagnostics failed:\n", traceback.format_exc())
        raise SystemExit(1)
