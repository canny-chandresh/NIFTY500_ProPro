# src/hc_runner.py
# Safe wrapper around your existing src/healthcheck.py

from __future__ import annotations
import sys, json, traceback
from pathlib import Path

def main(send_telegram: bool = True) -> int:
    report_dir = Path("reports/debug"); report_dir.mkdir(parents=True, exist_ok=True)
    status = {"ok": False, "sent_tg": False, "errors": []}

    try:
        sys.path.append("src")
        import healthcheck  # your existing file

        # Prefer main(), else run()
        if hasattr(healthcheck, "main"):
            rc = healthcheck.main(send_telegram=send_telegram)  # type: ignore[arg-type]
        elif hasattr(healthcheck, "run"):
            rc = healthcheck.run(send_telegram=send_telegram)   # type: ignore[attr-defined]
        else:
            raise RuntimeError("healthcheck imported, but no main() or run() found.")

        status["ok"] = (rc is None) or (rc == 0)
        status["sent_tg"] = bool(send_telegram)

    except Exception as e:
        status["errors"].append(repr(e))
        with open(report_dir / "healthcheck_traceback.txt", "w") as f:
            traceback.print_exc(file=f)

    # Always write a machine-readable JSON snapshot too
    with open(report_dir / "healthcheck_status.json", "w") as f:
        json.dump(status, f, indent=2)

    # Also print to logs
    print(json.dumps(status, indent=2))
    return 0

if __name__ == "__main__":
    # default = send Telegram in manual runs
    sys.exit(main(send_telegram=True))
