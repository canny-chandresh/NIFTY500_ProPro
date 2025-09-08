# -*- coding: utf-8 -*-
from __future__ import annotations
import os, re, json, shutil, traceback, datetime as dt
from pathlib import Path
from typing import Dict, Any, List, Tuple

CONFIG = None
def _load_config():
    global CONFIG
    if CONFIG is None:
        try:
            from config import CONFIG as C
            CONFIG = C
        except Exception:
            CONFIG = {}

ROOT = Path(".")
REPORTS = ROOT / "reports" / "debug"
REPORTS.mkdir(parents=True, exist_ok=True)
DLAKE = ROOT / "datalake"

def _now_stamp():
    return dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")

def _write_json(path: Path, obj: Any):
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(obj, indent=2))
    except Exception:
        pass

def _read_text_safe(path: Path, default=""):
    try:
        return path.read_text(errors="ignore")
    except Exception:
        return default

def _list_recent(paths: List[Path], limit=5) -> List[Tuple[str, str]]:
    out=[]
    for p in paths:
        if p.exists():
            out.append((str(p), _read_text_safe(p)[:100000]))
    return out[:limit]

class FixAction:
    def __init__(self, name: str, why: str, fn):
        self.name = name; self.why = why; self.fn = fn

def fix_import_relative():
    _load_config()
    guard = ROOT / "src" / "_path_guard.py"
    guard.write_text("import sys, os\np=os.path.join(os.getcwd(),'src')\n"
                     "sys.path.append(p) if p not in sys.path else None\n")
    CONFIG.setdefault("diagnostics", {}).setdefault("echo_pythonpath", True)
    return {"wrote": str(guard)}

def fix_indentation():
    return {"advice": "IndentationError: re-paste offending file fully."}

def fix_telegram_400():
    _load_config()
    CONFIG.setdefault("notify", {}).setdefault("force_every_run", True)
    return {"set": {"notify.force_every_run": True}}

def fix_yf_rate_limit():
    _load_config()
    ing = CONFIG.setdefault("ingest", {})
    ing.setdefault("rate_limit_sec", 1.5)
    intr = ing.setdefault("intraday", {})
    intr.setdefault("max_symbols", max(50, int(intr.get("max_symbols", 200)) // 2))
    return {"set": {"ingest.rate_limit_sec": ing["rate_limit_sec"], "ingest.intraday.max_symbols": intr["max_symbols"]}}

def fix_spec_missing():
    from matrix import _load_spec, SPEC_FILE
    spec = _load_spec()
    return {"spec_exists": SPEC_FILE.exists(), "keep_len": len(spec.get("keep", []))}

def fix_datalake_missing():
    for p in [DLAKE, DLAKE/"intraday"/"5m", DLAKE/"macro", DLAKE/"options_chain",
              DLAKE/"features_runtime"/"boosters", DLAKE/"features_runtime"/"dl_ft",
              DLAKE/"features_runtime"/"dl_tcn", DLAKE/"features_runtime"/"dl_tst",
              DLAKE/"features_runtime"/"calibration", DLAKE/"features_runtime"/"meta"]:
        p.mkdir(parents=True, exist_ok=True)
    return {"created": True}

def fix_options_rate_limit():
    _load_config()
    CONFIG.setdefault("ingest", {}).setdefault("options", {}).setdefault("enabled", False)
    return {"set": {"ingest.options.enabled": False}}

REGISTRY: List[Tuple[re.Pattern, FixAction]] = [
    (re.compile(r"attempted relative import with no known parent package", re.I), FixAction("import_path_guard","missing src in sys.path", fix_import_relative)),
    (re.compile(r"IndentationError", re.I), FixAction("indentation_error","bad paste", fix_indentation)),
    (re.compile(r"HTTP 400.*telegram", re.I), FixAction("telegram_plain_retry","Telegram HTML", fix_telegram_400)),
    (re.compile(r"Too Many Requests|rate limit|HTTP 429", re.I), FixAction("ingest_backoff","source throttling", fix_yf_rate_limit)),
    (re.compile(r"feature_spec\.yaml.*(No such file|not found)", re.I), FixAction("rebuild_feature_spec","missing spec", fix_spec_missing)),
    (re.compile(r"(FileNotFoundError:.*datalake)", re.I), FixAction("datalake_bootstrap","missing datalake", fix_datalake_missing)),
    (re.compile(r"(NSE.*options.*rate)|(optionchain.*error)", re.I), FixAction("options_backoff","options throttled", fix_options_rate_limit)),
]

def scan_logs_for_errors() -> str:
    candidates = []
    candidates += list((REPORTS).glob("errors_*.txt"))
    candidates += list((REPORTS).glob("errors_*.json"))
    candidates += list((ROOT/"reports").rglob("eod_report.txt"))
    text=[]
    for p, snippet in _list_recent(candidates, limit=8):
        text.append(f"--- {p} ---\n{snippet}\n")
    return "\n".join(text)[:200000]

def suggest_and_apply(blob: str):
    applied=[]; suggestions=[]
    for pat, action in REGISTRY:
        if pat.search(blob):
            try:
                res = action.fn() or {}
                applied.append({"action": action.name, "why": action.why, "result": res})
            except Exception as e:
                suggestions.append({"action": action.name, "why": action.why, "error": repr(e)})
    return {"applied": applied, "suggestions": suggestions}

def main():
    try:
        blob = scan_logs_for_errors()
        out = suggest_and_apply(blob) if blob.strip() else {"status":"no_logs"}
        path = REPORTS / f"auto_fix_{_now_stamp()}.json"
        _write_json(path, {"src": blob[:4000], "out": out})
        return {"ok": True, "report": str(path)}
    except Exception as e:
        path = REPORTS / f"auto_fix_exc_{_now_stamp()}.json"
        _write_json(path, {"error": repr(e)})
        return {"ok": False, "report": str(path)}
