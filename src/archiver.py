# src/archiver.py
from __future__ import annotations
import os, sys, json, time, zipfile, shutil
from pathlib import Path
from datetime import datetime, timedelta

# -------- settings --------
DATA_ROOT   = Path("datalake")
ARCH_ROOT   = Path("archives")
INDEX_FILE  = ARCH_ROOT / "archive_index.json"

# Defaults: 24 months ~ 730 days; override via env ARCHIVER_RETENTION_MONTHS
RETENTION_MONTHS = int(os.getenv("ARCHIVER_RETENTION_MONTHS", "24"))
# Safe, simple day approximation for 24 months
RETENTION_DAYS   = int(round(RETENTION_MONTHS * 30.42))  # ≈ 730 for 24 months

# Only archive typical data artifacts; add more as needed
ARCHIVE_EXTS = {".csv", ".parquet", ".json", ".feather", ".pq", ".gz"}

# Dry-run support: set ARCHIVER_DRY_RUN=true to preview without changing files
DRY_RUN = str(os.getenv("ARCHIVER_DRY_RUN", "false")).strip().lower() in ("1","true","yes","y","on")

def _now_utc() -> datetime:
    # We use UTC for repeatability on GitHub runners
    return datetime.utcnow()

def _cutoff_dt() -> datetime:
    # Use days approximation (robust across environments w/o dateutil)
    return _now_utc() - timedelta(days=RETENTION_DAYS)

def _month_key(ts: float) -> str:
    dt = datetime.utcfromtimestamp(ts)
    return f"{dt.year:04d}-{dt.month:02d}"

def _load_index() -> list:
    if INDEX_FILE.exists():
        try:
            return json.load(INDEX_FILE.open())
        except Exception:
            pass
    return []

def _save_index(idx: list):
    ARCH_ROOT.mkdir(parents=True, exist_ok=True)
    json.dump(idx[-5000:], INDEX_FILE.open("w"), indent=2)  # cap to last 5k entries

def _should_archive(path: Path, cutoff_ts: float) -> bool:
    if not path.is_file():
        return False
    if path.suffix.lower() not in ARCHIVE_EXTS:
        return False
    try:
        ts = path.stat().st_mtime
    except FileNotFoundError:
        return False
    return ts < cutoff_ts

def _zip_target(month_key: str) -> Path:
    ARCH_ROOT.mkdir(parents=True, exist_ok=True)
    return ARCH_ROOT / f"{month_key}.zip"

def _add_to_zip(zip_path: Path, src_file: Path, arcname: str):
    # append-create mode
    with zipfile.ZipFile(zip_path, mode="a", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.write(src_file, arcname=arcname)

def _rel_from_datalake(p: Path) -> str:
    try:
        return str(p.relative_to(DATA_ROOT))
    except ValueError:
        return p.name

def _walk_candidates(cutoff_ts: float):
    if not DATA_ROOT.exists():
        return
    for root, dirs, files in os.walk(DATA_ROOT):
        # skip any nested "archives" mistakenly placed inside datalake
        dirs[:] = [d for d in dirs if d.lower() != "archives"]
        for fn in files:
            fp = Path(root) / fn
            if _should_archive(fp, cutoff_ts):
                yield fp

def run_archiver(retention_months: int | None = None, dry_run: bool | None = None) -> dict:
    """
    Archives files older than cutoff into monthly ZIPs under archives/.
    Returns summary dict with counts and bytes moved.
    """
    if retention_months is not None:
        cutoff = _now_utc() - timedelta(days=int(round(retention_months * 30.42)))
    else:
        cutoff = _cutoff_dt()
    cutoff_ts = cutoff.timestamp()

    if dry_run is None:
        dry_run = DRY_RUN

    ARCH_ROOT.mkdir(parents=True, exist_ok=True)

    moved = 0
    bytes_moved = 0
    errors = []
    index = _load_index()

    for fpath in _walk_candidates(cutoff_ts):
        try:
            st = fpath.stat()
            month = _month_key(st.st_mtime)
            zip_path = _zip_target(month)
            rel = _rel_from_datalake(fpath)
            if dry_run:
                print(f"[DRY] would archive → {fpath}  ->  {zip_path}:{rel}")
                moved += 1
                bytes_moved += st.st_size
                continue

            # ensure parent dirs exist in arcname prefix
            arcname = rel.replace("\\", "/")  # normalize for zip
            _add_to_zip(zip_path, fpath, arcname=arcname)

            # remove original only after successful zip write
            size = st.st_size
            fpath.unlink(missing_ok=True)

            # record in index
            index.append({
                "archived_at_utc": _now_utc().isoformat() + "Z",
                "month_bucket": month,
                "zip": str(zip_path),
                "source_rel": rel,
                "size_bytes": size
            })
            moved += 1
            bytes_moved += size

        except Exception as e:
            errors.append({"file": str(fpath), "err": repr(e)})

    # Save index if not dry run
    if not dry_run:
        _save_index(index)

    return {
        "dry_run": dry_run,
        "retention_months": retention_months or RETENTION_MONTHS,
        "cutoff_utc": cutoff.isoformat() + "Z",
        "files_archived": moved,
        "bytes_archived": bytes_moved,
        "errors": errors
    }

if __name__ == "__main__":
    # CLI usage:
    #   python -m archiver            # uses default retention (ENV or 24m)
    #   ARCHIVER_DRY_RUN=true python -m archiver
    #   ARCHIVER_RETENTION_MONTHS=18 python -m archiver
    summary = run_archiver()
    print(json.dumps(summary, indent=2))
