# src/quality_checks.py
from __future__ import annotations
import json, datetime as dt
from pathlib import Path
import pandas as pd
import numpy as np

DL = Path("datalake")
FEAT_DIR = DL / "features"
REP = Path("reports/metrics"); REP.mkdir(parents=True, exist_ok=True)

def _safe_read(p: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(p, parse_dates=[c for c in ["Date","date","timestamp"] if c in open(p).readline()])
    except Exception:
        try: return pd.read_csv(p)
        except Exception: return pd.DataFrame()

def check_no_future_timestamps(df: pd.DataFrame, date_col="Date") -> dict:
    if date_col not in df.columns: return {"ok": True, "reason":"no_date_col"}
    future = (pd.to_datetime(df[date_col], errors="coerce") > pd.Timestamp.utcnow()+pd.Timedelta(minutes=1)).sum()
    return {"ok": future == 0, "future_rows": int(future)}

def check_target_lookahead(df: pd.DataFrame, date_col="Date") -> dict:
    """
    Heuristic: targets must be constructed via forward shift (-N).
    We can’t see internals, but we can check that:
      - targets are uncorrelated with same-row HL/Close beyond small noise
      - or that targets don't equal contemporaneous returns (common leakage)
    """
    tcols = [c for c in df.columns if c.startswith("y_")]
    if not tcols: return {"ok": True, "reason":"no_targets"}
    issues = []
    for t in tcols:
        if {"High","Low","Close"} <= set(df.columns):
            # correlation vs contemporaneous return as leakage hint:
            r_now = df["Close"].pct_change()
            corr = float(pd.concat([r_now, df[t]], axis=1).corr().iloc[0,1])
            if np.isfinite(corr) and abs(corr) > 0.5:
                issues.append({"target": t, "corr_now_ret": corr})
    return {"ok": len(issues)==0, "issues": issues}

def check_missing_and_flags(df: pd.DataFrame) -> dict:
    miss = df.isna().mean().sort_values(ascending=False)
    top = miss.head(10).to_dict()
    # verify that for any feature X with NaNs, there exists X_is_missing flag
    nan_feats = [c for c in df.columns if df[c].isna().any() and not c.endswith("_is_missing")]
    missing_flags = [f"{c}_is_missing" for c in nan_feats if f"{c}_is_missing" in df.columns]
    return {"ok": len(nan_feats)==0 or len(missing_flags)>0, "nan_top": top, "flags_present_for_some": len(missing_flags)>0}

def run_quality_on_symbol(symbol_csv: Path) -> dict:
    df = _safe_read(symbol_csv)
    if df.empty: return {"symbol": symbol_csv.stem, "ok": False, "reason": "empty"}
    out = {"symbol": symbol_csv.stem}
    out["no_future_ts"] = check_no_future_timestamps(df)
    out["no_lookahead_targets"] = check_target_lookahead(df)
    out["missing_policy"] = check_missing_and_flags(df)
    out["ok"] = out["no_future_ts"]["ok"] and out["no_lookahead_targets"]["ok"] and out["missing_policy"]["ok"]
    return out

def run_all(limit:int|None=None) -> dict:
    files = list(FEAT_DIR.glob("*_features.csv"))
    if limit: files = files[:limit]
    results = [run_quality_on_symbol(p) for p in files]
    summary = {
        "when_utc": dt.datetime.utcnow().isoformat()+"Z",
        "checked": len(results),
        "passed": sum(1 for r in results if r.get("ok")),
        "failed": sum(1 for r in results if not r.get("ok")),
        "by_symbol": results
    }
    (REP / "quality_report.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary

if __name__ == "__main__":
    print(json.dumps(run_all(), indent=2))
