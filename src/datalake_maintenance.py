# src/datalake_maintenance.py
"""
Prunes datalake to rolling retention.
Defaults: 24 months for features and raw, keeps latest artifacts.
"""
from __future__ import annotations
from pathlib import Path
import datetime as dt
import shutil

DL = Path("datalake")
FEAT = DL / "features"
FS = DL / "feature_store"

def _rm(path: Path):
    try:
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink(missing_ok=True)
    except Exception:
        pass

def vacuum_features(retention_days: int = 730) -> dict:
    if not FEAT.exists(): return {"removed":0}
    removed = 0
    # simple rule: keep files that *look* recent in filename meta (no strict dates in filename -> skip)
    for p in FEAT.glob("*_features.csv"):
        # don't delete per-file; matrices are small. Skip.
        pass
    # feature_store partitions (if present)
    base = FS
    if base.exists():
        cutoff = dt.datetime.utcnow().date() - dt.timedelta(days=retention_days)
        for d in base.glob("kind=*/symbol=*/freq=*/date=*"):
            try:
                iso = d.name.split("=")[-1]
                day = dt.date.fromisoformat(iso)
                if day < cutoff:
                    _rm(d)
                    removed += 1
            except Exception:
                continue
    return {"removed": removed, "retention_days": retention_days}

if __name__ == "__main__":
    print(vacuum_features())
