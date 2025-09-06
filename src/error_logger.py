# src/error_logger.py
"""
Comprehensive run logger for NIFTY500_ProPro.

Features:
- Captures stdout/stderr and exceptions with full tracebacks.
- Records environment snapshot, phase timings, HTTP endpoints touched (redacted).
- Snapshots resource usage before/after (RSS memory, file descriptors, threads, sockets).
- Flags spikes; optionally FAILS the run when thresholds exceeded.
- Rotates logs/manifests to keep storage bounded.
- Prunes datalake/paper_trades.csv to last N days (default 90).

Config via environment variables (all optional):
  ERRORLOG_HTTP_PROBE=true|false        # wrap requests to list endpoints (default: true)
  ERRORLOG_MAX_LOGS=50                  # keep this many *.log (default: 50)
  ERRORLOG_MAX_MANIFESTS=200            # keep this many manifests (default: 200)
  ERRORLOG_TRADE_RETENTION_DAYS=90      # prune paper_trades to last N days (default: 90)
  ERRORLOG_FAIL_ON_SPIKE=true|false     # exit non-zero if big spikes (default: false)
  ERRORLOG_MEM_SPIKE_MB=500             # memory RSS spike MB to fail (default: 500)
  ERRORLOG_FD_SPIKE=50                  # FD spike to fail (default: 50)
  ERRORLOG_THREAD_SPIKE=100             # thread spike to fail (default: 100)
  ERRORLOG_CONN_SPIKE=100               # socket spike to fail (default: 100)
"""
from __future__ import annotations
import os, sys, io, re, json, time, traceback, datetime as dt, platform, subprocess, glob
from contextlib import contextmanager

try:
    import psutil
except Exception:
    psutil = None

try:
    import pandas as pd
except Exception:
    pd = None

LOG_DIR = "reports/logs"
MET_DIR = "reports/metrics"
DL_DIR  = "datalake"
PAPER_TRADES = os.path.join(DL_DIR, "paper_trades.csv")

def _getenv_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None: return default
    return str(v).strip().lower() in ("1","true","yes","y","on")

def _getenv_int(name: str, default: int) -> int:
    try: return int(os.getenv(name, "").strip())
    except Exception: return default

RE_PATTERNS = [
    (re.compile(r'(ghp_[A-Za-z0-9]{20,})'), '***REDACTED_GH_TOKEN***'),
    (re.compile(r'(?i)(Bearer)\s+([A-Za-z0-9\-\._~\+\/]+=*)'), r'\1 ***REDACTED***'),
    (re.compile(r'(?i)(TG_BOT_TOKEN|TG_CHAT_ID)\s*=\s*["\']?([^\s"\'\\]+)'), r'\1=***REDACTED***'),
    (re.compile(r'([\w\.-]+@[\w\.-]+\.\w+)'), '***REDACTED_EMAIL***'),
    (re.compile(r'(?i)(token|key|auth|signature|sig|secret)=([^&\s]+)'), r'\1=***REDACTED***'),
]
def _redact(s: str) -> str:
    if not s: return s
    out = s
    for pat, repl in RE_PATTERNS:
        out = pat.sub(repl, out)
    return out

def _utc_stamp() -> str:
    return dt.datetime.utcnow().strftime("%Y%m%d_%H%M%SZ")

def _proc_stats():
    out = {"rss_mb": None,"fds": None,"threads": None,"conns": None}
    try:
        if psutil:
            p = psutil.Process(os.getpid())
            out["rss_mb"] = round(p.memory_info().rss / (1024**2), 2)
            try: out["fds"] = p.num_fds()
            except Exception: pass
            out["threads"] = p.num_threads()
            try: out["conns"] = len(p.connections(kind='inet'))
            except Exception: pass
    except Exception:
        pass
    return out

def _env_snapshot():
    snap = {
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "executable": sys.executable,
        "cwd": os.getcwd(),
        "packages": {}
    }
    for mod in ("pandas","numpy","pyarrow","requests","ta","yfinance"):
        try:
            m = __import__(mod)
            ver = getattr(m, "__version__", "unknown")
            snap["packages"][mod] = ver
        except Exception:
            pass
    try:
        sha = subprocess.check_output(["git","rev-parse","HEAD"], timeout=3).decode().strip()
        snap["git_sha"] = sha
    except Exception:
        pass
    return snap

class _HTTPProbe:
    def __init__(self):
        self.enabled = False
        self.calls = []
        self._orig = None
    def _safe_url(self, url: str) -> str:
        try:
            return re.sub(r'\?.*$', '', str(url))
        except Exception:
            return str(url)
    def _wrap(self, func):
        def wrapped(session, method, url, *a, **kw):
            try:
                self.calls.append({"method": str(method).upper(), "url": self._safe_url(url)})
            except Exception:
                pass
            return func(session, method, url, *a, **kw)
        return wrapped
    def enable(self):
        if self.enabled: return
        try:
            import requests
            self._orig = requests.sessions.Session.request
            requests.sessions.Session.request = self._wrap(self._orig)
            self.enabled = True
        except Exception:
            self.enabled = False
    def disable(self):
        if not self.enabled: return
        try:
            import requests
            if self._orig:
                requests.sessions.Session.request = self._orig
        except Exception:
            pass
        self.enabled = False

def _rotate_dir(pattern: str, keep: int):
    try:
        files = sorted(glob.glob(pattern), key=lambda p: os.path.getmtime(p), reverse=True)
        for old in files[keep:]:
            try: os.remove(old)
            except Exception: pass
    except Exception:
        pass

def _prune_paper_trades(days: int) -> dict:
    info = {"pruned": False, "kept_rows": None, "total_rows": None, "error": None}
    if not pd or not os.path.exists(PAPER_TRADES):
        return info
    try:
        df = pd.read_csv(PAPER_TRADES)
        info["total_rows"] = len(df)
        if "when_utc" not in df.columns or df.empty:
            return info
        def _parse(x):
            try: return dt.datetime.fromisoformat(str(x).replace("Z",""))
            except Exception: return None
        df["__ts"] = df["when_utc"].map(_parse)
        df = df.dropna(subset=["__ts"])
        cutoff = dt.datetime.utcnow() - dt.timedelta(days=days)
        df2 = df[df["__ts"] >= cutoff].drop(columns=["__ts"])
        info["kept_rows"] = len(df2)
        if info["kept_rows"] < info["total_rows"]:
            df2.to_csv(PAPER_TRADES, index=False)
            info["pruned"] = True
        return info
    except Exception as e:
        info["error"] = repr(e)
        return info

class RunLogger:
    """
    Captures stdout/stderr, exceptions, phase timings, env snapshot,
    resource deltas, optional HTTP call probe. Writes:
      - reports/logs/<label>_<run_id>.log
      - reports/metrics/run_manifest_<run_id>.json
      - reports/metrics/run_history.json (rolling)
    Also rotates logs/manifests and prunes paper_trades.csv.
    """
    def __init__(self, label: str = "run", run_id: str | None = None, http_probe: bool | None = None):
        self.label = label
        self.run_id = run_id or f"{label}_{_utc_stamp()}"
        self.buf = io.StringIO()
        self.errors = []
        self.started_utc = dt.datetime.utcnow().isoformat()+"Z"
        self.pre_stats = _proc_stats()
        self.env = _env_snapshot()
        self.phases = []
        self.kv = {}
        if http_probe is None:
            http_probe = _getenv_bool("ERRORLOG_HTTP_PROBE", True)
        self.http = _HTTPProbe()
        if http_probe: self.http.enable()

        self.max_logs = _getenv_int("ERRORLOG_MAX_LOGS", 50)
        self.max_manifests = _getenv_int("ERRORLOG_MAX_MANIFESTS", 200)
        self.trade_retention_days = _getenv_int("ERRORLOG_TRADE_RETENTION_DAYS", 90)
        self.fail_on_spike = _getenv_bool("ERRORLOG_FAIL_ON_SPIKE", False)
        self.mem_spike_mb = _getenv_int("ERRORLOG_MEM_SPIKE_MB", 500)
        self.fd_spike = _getenv_int("ERRORLOG_FD_SPIKE", 50)
        self.thread_spike = _getenv_int("ERRORLOG_THREAD_SPIKE", 100)
        self.conn_spike = _getenv_int("ERRORLOG_CONN_SPIKE", 100)

        os.makedirs(LOG_DIR, exist_ok=True)
        os.makedirs(MET_DIR, exist_ok=True)
        os.makedirs(DL_DIR, exist_ok=True)

    from contextlib import contextmanager
    @contextmanager
    def section(self, name: str, swallow: bool = True):
        t0 = time.time()
        self._write(f"\n=== [SECTION START] {name} ===\n")
        err = None
        try:
            yield
        except Exception as e:
            err = repr(e)
            self._write("\n--- EXCEPTION (section) ---\n" + traceback.format_exc() + "\n")
            self.errors.append({"section": name, "error": repr(e), "traceback": traceback.format_exc()})
            if not swallow: raise
        finally:
            t1 = time.time()
            self.phases.append({"name": name, "t0": t0, "t1": t1, "secs": round(t1-t0,3), "error": err})
            self._write(f"=== [SECTION END] {name} ({round(t1-t0,3)}s) ===\n")

    @contextmanager
    def capture_all(self, section: str | None = None, swallow: bool = True):
        old_out, old_err = sys.stdout, sys.stderr
        sys.stdout = sys.stderr = self.buf
        self._write(f"=== BEGIN [{section or self.label}] @ {self.started_utc} ===\n")
        try:
            yield
        except Exception as e:
            tb = traceback.format_exc()
            self.errors.append({"section": section or self.label, "error": repr(e), "traceback": tb})
            self._write("\n--- EXCEPTION CAUGHT ---\n" + tb + "\n")
            if not swallow: raise
        finally:
            self._write(f"=== END [{section or self.label}] ===\n")
            sys.stdout, sys.stderr = old_out, old_err

    def add_meta(self, **kwargs):
        self.kv.update(kwargs or {})

    def _write(self, s: str):
        try: self.buf.write(s)
        except Exception: pass

    def dump(self) -> str:
        post_stats = _proc_stats()
        try: self.http.disable()
        except Exception: pass

        def diff(a, b):
            if a is None or b is None: return None
            return b - a

        drss = diff(self.pre_stats.get("rss_mb"), post_stats.get("rss_mb"))
        dfds = diff(self.pre_stats.get("fds"), post_stats.get("fds"))
        dthr = diff(self.pre_stats.get("threads"), post_stats.get("threads"))
        dcnn = diff(self.pre_stats.get("conns"), post_stats.get("conns"))
        leak_flags = []
        if drss is not None and drss >= 200: leak_flags.append(f"memory_rss_spike_mb={drss}")
        if dfds is not None and dfds >= 10:  leak_flags.append(f"fd_leak_count={dfds}")
        if dthr is not None and dthr >= 20:  leak_flags.append(f"thread_leak_count={dthr}")
        if dcnn is not None and dcnn >= 20:  leak_flags.append(f"socket_leak_count={dcnn}")

        ended = dt.datetime.utcnow().isoformat()+"Z"
        http_calls = getattr(self.http, "calls", []) or []
        summary = {
            "run_id": self.run_id,
            "label": self.label,
            "started_utc": self.started_utc,
            "ended_utc": ended,
            "errors_count": len(self.errors),
            "phases": self.phases,
            "pre_stats": self.pre_stats,
            "post_stats": post_stats,
            "delta": {"rss_mb": drss, "fds": dfds, "threads": dthr, "conns": dcnn},
            "leak_flags": leak_flags,
            "env": self.env,
            "http_calls": http_calls[:200],
            "meta": self.kv,
        }

        manifest_path = os.path.join(MET_DIR, f"run_manifest_{self.run_id}.json")
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)

        hist_path = os.path.join(MET_DIR, "run_history.json")
        try:
            hist = json.load(open(hist_path)) if os.path.exists(hist_path) else []
        except Exception:
            hist = []
        hist.append({k: summary[k] for k in ("run_id","label","started_utc","ended_utc","errors_count","leak_flags")})
        hist = hist[-200:]
        with open(hist_path, "w", encoding="utf-8") as f:
            json.dump(hist, f, indent=2)

        os.makedirs(LOG_DIR, exist_ok=True)
        log_path = os.path.join(LOG_DIR, f"{self.label}_{self.run_id}.log")
        body = _redact(self.buf.getvalue())
        with open(log_path, "w", encoding="utf-8") as f:
            f.write(body)
            f.write("\n\n=== SUMMARY JSON ===\n")
            f.write(_redact(json.dumps(summary, indent=2)))
            if self.errors:
                f.write("\n--- ERROR LIST ---\n")
                for i, e in enumerate(self.errors, 1):
                    f.write(_redact(json.dumps({
                        "i": i, "section": e.get("section"), "error": e.get("error"),
                        "traceback": e.get("traceback","")
                    }, indent=2)) + "\n")

        if self.errors:
            err_path = os.path.join(LOG_DIR, f"errors_only_{self.run_id}.txt")
            with open(err_path, "w", encoding="utf-8") as f:
                for i, e in enumerate(self.errors, 1):
                    f.write(f"[{i}] section={e.get('section')} err={e.get('error')}\n")
                    f.write(_redact(e.get("traceback","")) + "\n")

        _rotate_dir(os.path.join(LOG_DIR, "*.log"), self.max_logs)
        _rotate_dir(os.path.join(MET_DIR, "run_manifest_*.json"), self.max_manifests)

        prune_info = _prune_paper_trades(self.trade_retention_days)
        if prune_info.get("pruned"):
            print(f"[RunLogger] Pruned paper_trades.csv to last {self.trade_retention_days} days "
                  f"({prune_info.get('kept_rows')}/{prune_info.get('total_rows')} rows).")

        if self.fail_on_spike:
            fail = False; reasons = []
            if drss is not None and drss >= self.mem_spike_mb:
                fail = True; reasons.append(f"RSS+{drss}MB>={self.mem_spike_mb}")
            if dfds is not None and dfds >= self.fd_spike:
                fail = True; reasons.append(f"FD+{dfds}>={self.fd_spike}")
            if dthr is not None and dthr >= self.thread_spike:
                fail = True; reasons.append(f"THR+{dthr}>={self.thread_spike}")
            if dcnn is not None and dcnn >= self.conn_spike:
                fail = True; reasons.append(f"CONN+{dcnn}>={self.conn_spike}")
            if fail:
                with open(os.path.join(MET_DIR, "resource_spike_fail.txt"), "w") as f:
                    f.write(" ; ".join(reasons))
                print(f"[RunLogger] Failing run due to resource spikes: {', '.join(reasons)}")
                raise SystemExit(2)

        return log_path
