# src/feature_store.py
from __future__ import annotations
import json
from pathlib import Path
from typing import Dict, Optional, List

import pandas as pd

try:
    import yaml
    YAML_OK = True
except Exception:
    YAML_OK = False

try:
    import pyarrow as pa  # noqa: F401
    import pyarrow.parquet as pq  # noqa: F401
    ARROW_OK = True
except Exception:
    ARROW_OK = False

def _root(cfg: Dict) -> Path:
    return Path(cfg.get("paths", {}).get("feature_store", "datalake/feature_store"))

def _meta_dir(root: Path) -> Path:
    d = root / "_meta"
    d.mkdir(parents=True, exist_ok=True)
    return d

def write_features(cfg: Dict, df: pd.DataFrame, symbol: str, version: str = "v1") -> Path:
    root = _root(cfg)
    root.mkdir(parents=True, exist_ok=True)
    fpath = root / f"{symbol}_{version}.parquet"
    if ARROW_OK:
        df.to_parquet(fpath, index=False)
    else:
        # fallback
        fpath = root / f"{symbol}_{version}.csv"
        df.to_csv(fpath, index=False)
    # write meta
    meta = {
        "symbol": symbol,
        "version": version,
        "rows": int(len(df)),
        "columns": list(map(str, df.columns)),
    }
    mdir = _meta_dir(root)
    if YAML_OK:
        (mdir / f"{symbol}_{version}.yaml").write_text(
            yaml.safe_dump(meta, sort_keys=False), encoding="utf-8"
        )
    else:
        (mdir / f"{symbol}_{version}.json").write_text(
            json.dumps(meta, indent=2), encoding="utf-8"
        )
    return fpath

def load_latest(cfg: Dict, symbol: str) -> Optional[pd.DataFrame]:
    root = _root(cfg)
    # prefer parquet
    for p in sorted(root.glob(f"{symbol}_*.parquet"), reverse=True):
        try:
            return pd.read_parquet(p)
        except Exception:
            continue
    for p in sorted(root.glob(f"{symbol}_*.csv"), reverse=True):
        try:
            return pd.read_csv(p)
        except Exception:
            continue
    return None

def list_available(cfg: Dict) -> List[str]:
    root = _root(cfg)
    syms = set()
    for p in root.glob("*.parquet"):
        syms.add(p.name.split("_")[0])
    for p in root.glob("*.csv"):
        syms.add(p.name.split("_")[0])
    return sorted(syms)

def validate_schema(cfg: Dict, required_cols: List[str]) -> Dict:
    root = _root(cfg)
    bad = []
    for sym in list_available(cfg):
        df = load_latest(cfg, sym)
        if df is None:
            bad.append({"symbol": sym, "error": "load_failed"})
            continue
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            bad.append({"symbol": sym, "missing": missing})
    rep = Path(cfg.get("paths", {}).get("reports", "reports")) / "hygiene"
    rep.mkdir(parents=True, exist_ok=True)
    (rep / "feature_store_validate.json").write_text(json.dumps({"bad": bad}, indent=2), encoding="utf-8")
    return {"bad": bad, "count": len(bad)}
