# src/locks.py
from __future__ import annotations
import os, time, json, datetime as dt
from pathlib import Path

LOCK_DIR = Path("reports/locks")
LOCK_DIR.mkdir(parents=True, exist_ok=True)

class RunLock:
    """
    Simple file lock to prevent overlapping runs on Actions.
    Auto-expires after 'ttl_sec' to avoid deadlocks on crashes.
    """
    def __init__(self, name: str, ttl_sec: int = 1800):
        self.name = name
        self.ttl = ttl_sec
        self.path = LOCK_DIR / f"{name}.lock"
        self.acquired = False

    def acquire(self) -> bool:
        now = time.time()
        if self.path.exists():
            try:
                meta = json.load(self.path.open())
                if now - float(meta.get("ts", 0)) < self.ttl:
                    return False  # still held
            except Exception:
                pass
            # stale; remove
            try: self.path.unlink()
            except Exception: pass
        try:
            json.dump({"ts": now, "pid": os.getpid(), "when": dt.datetime.utcnow().isoformat()+"Z"},
                      self.path.open("w"))
            self.acquired = True
            return True
        except Exception:
            return False

    def release(self):
        if self.acquired and self.path.exists():
            try: self.path.unlink()
            except Exception: pass
        self.acquired = False

    def __enter__(self):
        if not self.acquire():
            raise RuntimeError(f"lock_busy:{self.name}")
        return self

    def __exit__(self, exc_type, exc, tb):
        self.release()
