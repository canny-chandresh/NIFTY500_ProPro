# src/diagnostics.py
from __future__ import annotations
import os, json, traceback, glob, datetime as dt
from pathlib import Path

KEY_MODULES = [
    "config","pipeline","regime","feature_rules","indicators",
    "options_executor","futures_executor","report_eod","report_periodic",
    "kill_switch","telegram","model_selector","sector","smartmoney","utils_time"
]

REPORTS_DIR = "reports"
DL = "datalake"

def _import_status():
    errs = {}
    for m in KEY_MODULES:
        try:
            __import__(m)
        except Exception as e:
            errs[m] = repr(e)
    return errs

def _exists(path: str) -> bool:
    return os.path.exists(path)

def _head(path: str, n: int = 5) -> list[str]:
    try:
        lines = []
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for i, line in enumerate(f):
                if i >= n: break
                lines.append(line.rstrip("\n"))
        return lines
    except Exception:
        return []

def _count_csv(path: str) -> int:
    try:
        import pandas as pd
        return int(len(pd.read_csv(path)))
    except Exception:
        return 0

def run_self_audit() -> dict:
    os.makedirs(REPORTS_DIR, exist_ok=True)
    out = {
        "when_utc": dt.datetime.utcnow().isoformat()+"Z",
        "datalake_present": os.path.isdir(DL),
        "daily_equity_present": _exists(os.path.join(DL,"daily_equity.parquet")) or _exists(os.path.join(DL,"daily_equity.csv")),
        "per_symbol_count": len(glob.glob(os.path.join(DL,"per_symbol","*.csv"))),
        "import_errors": _import_status(),
        "files": {}
    }

    # Sources used (pipeline writes this)
    src_path = os.path.join(REPORTS_DIR, "sources_used.json")
    if _exists(src_path):
        try:
            out["sources_used"] = json.load(open(src_path))
        except Exception as e:
            out["sources_used_error"] = repr(e)

    # Key CSVs presence & counts
    for name, p in {
        "paper_trades": os.path.join(DL,"paper_trades.csv"),
        "options_paper": os.path.join(DL,"options_paper.csv"),
        "futures_paper": os.path.join(DL,"futures_paper.csv"),
    }.items():
        out["files"][name] = {
            "exists": _exists(p),
            "rows": _count_csv(p)
        }

    # Write JSON
    json.dump(out, open(os.path.join(REPORTS_DIR,"self_audit.json"), "w"), indent=2)

    # Human-readable error report
    lines = []
    lines.append(f"Self-Audit @ {out['when_utc']}")
    lines.append(f"datalake_present: {out['datalake_present']}")
    lines.append(f"daily_equity_present: {out['daily_equity_present']}")
    lines.append(f"per_symbol_count: {out['per_symbol_count']}")
    if out.get("sources_used"):
        lines.append("sources_used:")
        lines.append(json.dumps(out["sources_used"], indent=2))

    if out["import_errors"]:
        lines.append("\nImport errors:")
        for k, v in out["import_errors"].items():
            if v:
                lines.append(f"- {k}: {v}")

    # add small file heads
    for label, path in {
        "paper_trades.csv": os.path.join(DL,"paper_trades.csv"),
        "options_paper.csv": os.path.join(DL,"options_paper.csv"),
        "futures_paper.csv": os.path.join(DL,"futures_paper.csv"),
        "options_source.txt": os.path.join(REPORTS_DIR,"options_source.txt"),
        "futures_source.txt": os.path.join(REPORTS_DIR,"futures_source.txt"),
    }.items():
        lines.append(f"\n== head {label} ==")
        for h in _head(path, 8):
            lines.append(h)

    open(os.path.join(REPORTS_DIR,"error_report.txt"), "w").write("\n".join(lines))
    return out

def generate_all() -> None:
    try:
        run_self_audit()
    except Exception as e:
        # Write a last-resort crash note so you still get something
        open(os.path.join(REPORTS_DIR,"error_report.txt"), "a").write(
            f"\n[diagnostics crashed] {repr(e)}\n{traceback.format_exc()}\n"
        )
