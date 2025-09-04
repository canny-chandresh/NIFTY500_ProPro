# src/live_train.py
from __future__ import annotations
import os, json, datetime as dt
import pandas as pd

def _utc_now_iso():
    return dt.datetime.utcnow().isoformat() + "Z"

def _mtime(path: str) -> float | None:
    try: return os.path.getmtime(path)
    except Exception: return None

def _hours_since(ts: float | None) -> float:
    if ts is None: return 1e9
    return (dt.datetime.utcnow() - dt.datetime.utcfromtimestamp(ts)).total_seconds() / 3600.0

def ensure_equities_fresh(max_age_hours: float = 6.0) -> dict:
    """
    Make sure equity OHLCV is present and fresh enough for ML:
      - If daily_equity.(parquet|csv) is missing or older than max_age_hours,
        pull last 60d via livefeeds.refresh_equity_data().
      - Always writes reports/sources_used.json 'equities' entry.
    """
    from pathlib import Path
    os.makedirs("reports", exist_ok=True)
    eq_parq = "datalake/daily_equity.parquet"
    eq_csv  = "datalake/daily_equity.csv"

    age_h = min(_hours_since(_mtime(eq_parq)), _hours_since(_mtime(eq_csv)))
    info = {"equities_source": "cached", "rows": 0, "symbols": 0, "age_hours": round(age_h,2)}

    need_refresh = (not os.path.exists(eq_parq) and not os.path.exists(eq_csv)) or age_h > max_age_hours
    if need_refresh:
        try:
            from livefeeds import refresh_equity_data
            ret = refresh_equity_data(days=60, interval="1d")
            info.update(ret)   # equities_source, rows, symbols
        except Exception as e:
            info["equities_source"] = f"error:{type(e).__name__}"

    # merge into sources_used.json
    try:
        src_path = "reports/sources_used.json"
        data = {}
        if os.path.exists(src_path):
            import json as _json
            data = _json.load(open(src_path))
        data["equities"] = info
        json.dump(data, open(src_path, "w"), indent=2)
    except Exception:
        pass
    return info

def train_all_modes_if_available() -> dict:
    """
    Call model training functions if they exist.
    Safe no-ops if modules/methods are absent.
    Writes reports/train_run.json summary.
    """
    out = {"when": _utc_now_iso(), "trained": []}

    def _try(mod_name: str, fn_name: str, label: str, **kw):
        try:
            mod = __import__(mod_name, fromlist=[fn_name])
            fn  = getattr(mod, fn_name, None)
            if callable(fn):
                fn(**kw)  # your train function signature should tolerate **kw
                out["trained"].append(label)
        except Exception as e:
            out.setdefault("errors", {})[label] = f"{type(e).__name__}: {e}"

    # adjust to your repo’s trainers; all are optional calls
    _try("model_selector", "train_incremental_equity", "equity")
    _try("model_selector", "train_incremental_intraday", "intraday")
    _try("model_selector", "train_incremental_swing", "swing")
    _try("model_selector", "train_incremental_long", "long")
    _try("options_executor", "train_from_live_chain", "options")     # optional, only if you add it
    _try("futures_executor", "train_from_live_futures", "futures")   # optional

    os.makedirs("reports", exist_ok=True)
    json.dump(out, open("reports/train_run.json","w"), indent=2)
    return out
