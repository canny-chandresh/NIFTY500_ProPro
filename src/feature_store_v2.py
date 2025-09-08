# src/feature_store_v2.py
from __future__ import annotations
import json, datetime as dt
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

try:
    import pyarrow as pa  # noqa
    ARROW = True
except Exception:
    ARROW = False

def _root(cfg: Dict) -> Path:
    return Path(cfg.get("paths", {}).get("feature_store", "datalake/feature_store"))

def write_partitioned(cfg: Dict, df: pd.DataFrame, symbol: str, date_col: str = "Date") -> Dict:
    root = _root(cfg); root.mkdir(parents=True, exist_ok=True)
    if date_col not in df.columns:
        raise ValueError("date column missing")
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col])

    # partition by year/month for fast incremental reads
    years = df[date_col].dt.year.unique()
    count = 0
    for y in years:
        sub = df[df[date_col].dt.year == y]
        months = sub[date_col].dt.month.unique()
        for m in months:
            part = sub[sub[date_col].dt.month == m]
            pdir = root / f"symbol={symbol}/year={y:04d}/month={m:02d}"
            pdir.mkdir(parents=True, exist_ok=True)
            f = pdir / "data.parquet"
            try:
                if ARROW:
                    part.to_parquet(f, index=False)
                else:
                    part.to_csv(str(f).replace(".parquet", ".csv"), index=False)
                count += len(part)
            except Exception:
                pass

    meta = {"symbol": symbol, "rows": count, "last_write_utc": dt.datetime.utcnow().isoformat()+"Z"}
    (root / f"symbol={symbol}/_meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
    return {"ok": True, "rows": count}

def load_view(cfg: Dict, symbols: List[str], start: Optional[str]=None, end: Optional[str]=None) -> pd.DataFrame:
    root = _root(cfg)
    frames = []
    for s in symbols:
        base = root / f"symbol={s}"
        if not base.exists(): 
            continue
        # naive read: concat all partitions (fast enough for GH runners)
        for p in sorted(base.rglob("*.parquet")):
            try:
                df = pd.read_parquet(p)
            except Exception:
                # csv fallback
                c = Path(str(p).replace(".parquet",".csv"))
                if c.exists():
                    df = pd.read_csv(c)
                else:
                    continue
            df["symbol"] = s
            frames.append(df)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    if start:
        out = out[pd.to_datetime(out["Date"]) >= pd.to_datetime(start)]
    if end:
        out = out[pd.to_datetime(out["Date"]) < pd.to_datetime(end)]
    return out

def freshness_report(cfg: Dict) -> Dict:
    root = _root(cfg)
    recs = []
    for m in root.glob("symbol=*/_meta.json"):
        try:
            meta = json.loads(m.read_text())
            recs.append(meta)
        except Exception:
            continue
    rep = Path(cfg.get("paths", {}).get("reports","reports")) / "hygiene"
    rep.mkdir(parents=True, exist_ok=True)
    (rep / "feature_store_freshness.json").write_text(json.dumps({"symbols": recs}, indent=2), encoding="utf-8")
    return {"count": len(recs)}
