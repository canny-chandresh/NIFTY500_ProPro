# src/feature_store.py
from __future__ import annotations
import os, json, hashlib, datetime as dt
from pathlib import Path
from typing import Dict, List, Optional
import pandas as pd

CFG = {
    "root": "datalake/feature_store",
    "manifests": "datalake/feature_store/_manifests",
    "partitions": ["symbol", "freq"],  # partition by symbol + frequency
    "hash_cols": None,  # if None, hash all columns
    "retention_days": 730,  # keep ~24 months by default
}

ROOT  = Path(CFG["root"])
MNFS  = Path(CFG["manifests"])
ROOT.mkdir(parents=True, exist_ok=True)
MNFS.mkdir(parents=True, exist_ok=True)

def _hash_df(df: pd.DataFrame, cols: Optional[List[str]] = None) -> str:
    if cols is None: cols = list(df.columns)
    hx = hashlib.sha256(pd.util.hash_pandas_object(df[cols], index=False).values.tobytes()).hexdigest()
    return hx

def put(symbol: str, freq: str, df: pd.DataFrame, kind: str = "features", meta: Dict = None) -> Dict:
    """
    Write a dataframe to the store as parquet:
      datalake/feature_store/kind=features/symbol=SBIN/freq=1d/date=YYYY-MM-DD.parquet
    Also write/update a manifest json with hash + row count.
    """
    if df is None or df.empty:
        return {"ok": False, "reason": "empty"}

    # Require a Date/timestamp column
    date_col = "Date" if "Date" in df.columns else ("date" if "date" in df.columns else None)
    if not date_col:
        return {"ok": False, "reason": "no Date column"}

    df = df.sort_values(date_col).reset_index(drop=True)
    # daily partition by last date present
    last_day = pd.to_datetime(df[date_col].iloc[-1]).date().isoformat()

    # Build path
    base = ROOT / f"kind={kind}" / f"symbol={symbol}" / f"freq={freq}" / f"date={last_day}"
    base.mkdir(parents=True, exist_ok=True)
    fpath = base / "data.parquet"

    # Write parquet
    df.to_parquet(fpath, index=False)

    # Manifest
    h = _hash_df(df, CFG["hash_cols"])
    m = {
        "kind": kind, "symbol": symbol, "freq": freq, "date": last_day,
        "rows": int(len(df)), "cols": list(df.columns),
        "hash": h, "meta": meta or {}, "written_utc": dt.datetime.utcnow().isoformat()+"Z"
    }
    (MNFS / f"{kind}__{symbol}__{freq}__{last_day}.json").write_text(json.dumps(m, indent=2), encoding="utf-8")

    return {"ok": True, "path": str(fpath), "manifest": str((MNFS / f"{kind}__{symbol}__{freq}__{last_day}.json"))}

def latest(symbol: str, freq: str, kind: str = "features") -> Optional[pd.DataFrame]:
    """Load latest partition for a symbol/freq & kind."""
    base = ROOT / f"kind={kind}" / f"symbol={symbol}" / f"freq={freq}"
    if not base.exists(): return None
    dates = sorted([p.name.split("=")[-1] for p in base.glob("date=*")])
    if not dates: return None
    fpath = base / f"date={dates[-1]}" / "data.parquet"
    if not fpath.exists(): return None
    return pd.read_parquet(fpath)

def vacuum(retention_days: int = None) -> Dict:
    """Remove old partitions beyond retention_days. Non-fatal."""
    keep_days = int(CFG.get("retention_days")) if retention_days is None else int(retention_days)
    cutoff = dt.datetime.utcnow().date() - dt.timedelta(days=keep_days)
    removed = 0
    for p in ROOT.glob("kind=*/*/*/date=*"):
        try:
            d = dt.date.fromisoformat(p.name.split("=")[-1])
            if d < cutoff:
                for f in p.glob("*"): f.unlink(missing_ok=True)
                p.rmdir()
                removed += 1
        except Exception:
            pass
    return {"removed": removed, "cutoff": cutoff.isoformat()}

def list_symbols(kind: str = "features") -> List[str]:
    base = ROOT / f"kind={kind}"
    if not base.exists(): return []
    return [p.name.split("=")[-1] for p in base.glob("symbol=*")]
