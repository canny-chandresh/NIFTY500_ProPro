from __future__ import annotations
import os, json, datetime as dt

def _utc_now_iso(): return dt.datetime.utcnow().isoformat() + "Z"

def _mtime(path: str):
    try: return os.path.getmtime(path)
    except Exception: return None

def _age_hours(ts):
    if ts is None: return 9e9
    return (dt.datetime.utcnow() - dt.datetime.utcfromtimestamp(ts)).total_seconds()/3600.0

def ensure_equities_fresh(max_age_hours: float = 6.0) -> dict:
    """
    Ensure datalake/daily_equity.(parquet|csv) is present & fresh (<= max_age_hours).
    If missing/stale → pull last 60d from yfinance via livefeeds.refresh_equity_data().
    Merge result into reports/sources_used.json under 'equities'.
    """
    os.makedirs("reports", exist_ok=True)
    eqp = "datalake/daily_equity.parquet"
    eqc = "datalake/daily_equity.csv"
    age = min(_age_hours(_mtime(eqp)), _age_hours(_mtime(eqc)))
    info = {"equities_source":"cached", "rows":0, "symbols":0, "age_hours": round(age,2)}
    need = (not os.path.exists(eqp) and not os.path.exists(eqc)) or age > max_age_hours

    if need:
        try:
            from livefeeds import refresh_equity_data
            ret = refresh_equity_data(days=60, interval="1d")
            info.update(ret)
        except Exception as e:
            info["equities_source"] = f"error:{type(e).__name__}"

    # merge
    try:
        src_p = "reports/sources_used.json"
        data = {}
        if os.path.exists(src_p):
            data = json.load(open(src_p))
        data["equities"] = info
        json.dump(data, open(src_p, "w"), indent=2)
    except Exception:
        pass
    return info

def _try_call(mod, fn, label, out, **kw):
    try:
        m = __import__(mod, fromlist=[fn])
        f = getattr(m, fn, None)
        if callable(f):
            f(**kw)
            out["trained"].append(label)
    except Exception as e:
        out.setdefault("errors", {})[label] = f"{type(e).__name__}: {e}"

def train_all_modes_if_available() -> dict:
    """
    Try trainers if they exist (safe no-ops if absent).
    Writes reports/train_run.json.
    """
    out = {"when": _utc_now_iso(), "trained": []}
    _try_call("model_selector", "train_incremental_equity",   "equity",   out)
    _try_call("model_selector", "train_incremental_intraday", "intraday", out)
    _try_call("model_selector", "train_incremental_swing",    "swing",    out)
    _try_call("model_selector", "train_incremental_long",     "long",     out)
    _try_call("options_executor","train_from_live_chain",     "options",  out)
    _try_call("futures_executor","train_from_live_futures",   "futures",  out)

    os.makedirs("reports", exist_ok=True)
    json.dump(out, open("reports/train_run.json","w"), indent=2)
    return out
