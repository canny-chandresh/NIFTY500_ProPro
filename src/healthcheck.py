# -*- coding: utf-8 -*-
"""
healthcheck.py
Manual end-to-end sanity run:
 - (assumes workflows already installed deps)
 - optionally ingests data (handled by workflow)
 - validates datalake, features, alphas, engine artifacts
 - writes JSON to reports/debug/healthcheck.json
 - sends a concise Telegram message

Usage (from GH Actions or locally):
  python -m healthcheck
"""

from __future__ import annotations
import os, json, traceback, glob
from pathlib import Path
from datetime import datetime, timezone

# --- repo modules ---
try:
    from config import CONFIG
except Exception:
    # minimal fallback
    CONFIG = {
        "paths": {"datalake":"datalake", "reports":"reports", "models":"models"},
        "universe": [],
        "engines": {"ml":{"enabled":True}, "boosters":{"enabled":True}, "dl":{"enabled":True, "ft":{"enabled":True},"tcn":{"enabled":True},"tst":{"enabled":True}},
                    "calibration":{"enabled":True}, "stacker":{"enabled":True}},
    }

def _tznow():
    return datetime.now(timezone.utc).isoformat()

def _safe_import(modname: str):
    try:
        return __import__(modname, fromlist=["*"]), None
    except Exception as e:
        traceback.print_exc()
        return None, repr(e)

def _send_telegram(text: str):
    # Prefer existing telegram helper if present
    try:
        from telegram import send_message
        return send_message(text)
    except Exception:
        pass
    # Fallback direct call
    import json, urllib.request
    token = os.getenv("TG_BOT_TOKEN")
    chat_id = os.getenv("TG_CHAT_ID")
    if not token or not chat_id:
        return {"ok": False, "reason": "TG secrets missing"}
    try:
        data = json.dumps({"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}).encode("utf-8")
        req = urllib.request.Request(
            url=f"https://api.telegram.org/bot{token}/sendMessage",
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=20) as r:
            return {"ok": True, "status": r.status}
    except Exception as e:
        return {"ok": False, "reason": repr(e)}

def run(send_telegram: bool = True) -> dict:
    out = {"when_utc": _tznow()}
    dl = Path(CONFIG["paths"]["datalake"])
    rp = Path(CONFIG["paths"]["reports"])
    models_dir = Path(CONFIG["paths"].get("models", "models"))
    (rp / "debug").mkdir(parents=True, exist_ok=True)

    # ---------- datalake checks ----------
    out["datalake_present"] = dl.exists()
    out["daily_hot_exists"] = (dl / "daily_hot.parquet").exists()
    out["per_symbol_count"] = len(list((dl / "per_symbol").glob("*.csv"))) if dl.exists() else 0
    out["intra5_count"] = len(list((dl / "intraday" / "5m").glob("*.csv"))) if dl.exists() else 0
    out["macro_exists"] = (dl / "macro" / "macro.parquet").exists()

    # ---------- import & alpha checks ----------
    # feature_store + matrix + alpha.runtime
    fs, e_fs = _safe_import("feature_store")
    rt, e_rt = _safe_import("alpha.runtime")
    out["import_errors"] = {}
    if e_fs: out["import_errors"]["feature_store"] = e_fs
    if e_rt: out["import_errors"]["alpha.runtime"] = e_rt

    alpha_cols, matrix_rows = [], 0
    try:
        uni = CONFIG.get("universe", [])
        ff = fs.get_feature_frame(uni) if fs else None
        if ff is not None and not ff.empty and rt:
            df_alpha = rt.run_enabled_alphas(ff, fast_only=True)
            alpha_cols = [c for c in df_alpha.columns if c.startswith("alpha_")]
        # approximate stitched rows if matrix is available
        mx, e_mx = _safe_import("matrix")
        if mx and not e_mx and ff is not None:
            _, _, _, stitched = mx.build_matrix(ff)
            matrix_rows = len(stitched)
    except Exception as e:
        out["alpha_runtime_error"] = repr(e)
        traceback.print_exc()

    out["alpha_fast_count"] = len(alpha_cols)
    out["matrix_rows_est"] = matrix_rows

    # ---------- engine artifacts ----------
    def _exists(pattern: str) -> bool:
        return any(Path(p).exists() and Path(p).stat().st_size > 0 for p in glob.glob(pattern))

    eng = CONFIG.get("engines", {})
    engines = {
        "ML": eng.get("ml", {}).get("enabled", False),
        "Boost": eng.get("boosters", {}).get("enabled", False) and (_exists("models/boost_*.pkl") or _exists(str(models_dir / "boost_*.pkl"))),
        "FT": eng.get("dl", {}).get("enabled", False) and eng["dl"].get("ft", {}).get("enabled", False) and _exists(str(models_dir / "dl_ft.pt")),
        "TCN": eng.get("dl", {}).get("enabled", False) and eng["dl"].get("tcn", {}).get("enabled", False) and _exists(str(models_dir / "dl_tcn.pt")),
        "TST": eng.get("dl", {}).get("enabled", False) and eng["dl"].get("tst", {}).get("enabled", False) and _exists(str(models_dir / "dl_tst.pt")),
        "Calib": eng.get("calibration", {}).get("enabled", False) and Path(models_dir / "calibration").exists(),
        "Stacker": eng.get("stacker", {}).get("enabled", False) and _exists(str(models_dir / "stacker.pkl")),
    }
    out["engines"] = engines

    # ---------- footer / summary ----------
    def tick(ok: bool) -> str:
        return "✔" if ok else "⚠"

    footer = (
        f"📦 Health: DLake={'OK' if (out['datalake_present'] and out['daily_hot_exists']) else 'MISS'} "
        f"| per={out['per_symbol_count']} intra5={out['intra5_count']} "
        f"| α_fast={out['alpha_fast_count']} rows~{out['matrix_rows_est']}\n"
        f"Engines: ML {tick(True)} • Boost {tick(engines['Boost'])} • "
        f"FT {tick(engines['FT'])} • TCN {tick(engines['TCN'])} • TST {tick(engines['TST'])} • "
        f"Calib {tick(engines['Calib'])} • Stacker {tick(engines['Stacker'])}"
    )
    out["summary"] = footer

    # persist
    (rp / "debug" / "healthcheck.json").write_text(json.dumps(out, indent=2))

    # Telegram
    if send_telegram:
        _send_telegram("*Manual Healthcheck*\n" + "```\n" + footer + "\n```")

    print(json.dumps(out, indent=2))
    return out

if __name__ == "__main__":
    run(send_telegram=True)
