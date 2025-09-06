# src/error_logger.py
from __future__ import annotations
import os, sys, io, re, json, time, traceback, datetime as dt, platform, subprocess
from contextlib import contextmanager

# Optional diagnostics
try:
    import psutil
except Exception:
    psutil = None

LOG_DIR = "reports/logs"
MET_DIR = "reports/metrics"

# ---------- Redaction (tokens, emails, Bearer, etc.) ----------
RE_PATTERNS = [
    (re.compile(r'(ghp_[A-Za-z0-9]{20,})'), '***REDACTED_GH_TOKEN***'),
    (re.compile(r'(?i)(Bearer)\s+([A-Za-z0-9\-\._~\+\/]+=*)'), r'\1 ***REDACTED***'),
    (re.compile(r'(?i)(TG_BOT_TOKEN|TG_CHAT_ID)\s*=\s*["\']?([^\s"\'\\]+)'), r'\1=***REDACTED***'),
    (re.compile(r'([\w\.-]+@[\w\.-]+\.\w+)'), '***REDACTED_EMAIL***'),
    # redact query tokens like token=..., key=..., auth=...
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
    """Process snapshot (best effort)."""
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
    # git SHA if available
    try:
        sha = subprocess.check_output(["git","rev-parse","HEAD"], timeout=3).decode().strip()
        snap["git_sha"] = sha
    except Exception:
        pass
    return snap

# ---------- Optional HTTP probe (counts endpoints safely) ----------
class _HTTPProbe:
    def __init__(self):
        self.enabled = False
        self.calls = []
        self._orig = None

    def _safe_url(self, url: str) -> str:
        # strip query string
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

# ---------- RunLogger ----------
class RunLogger:
    """
    Captures stdout/stderr, exceptions, phase timings, env snapshot,
    resource deltas, optional HTTP call probe. Writes:
      - reports/logs/<label>_<run_id>.log (full redacted log + JSON summary)
      - reports/metrics/run_manifest_<run_id>.json (machine-readable)
      - reports/metrics/run_history.json (rolling 200)
    """
    def __init__(self, label: str = "run", run_id: str | None = None, http_probe: bool = True):
        self.label = label
        self.run_id = run_id or f"{label}_{_utc_stamp()}"
        self.buf = io.StringIO()
        self.errors = []
        self.started_utc = dt.datetime.utcnow().isoformat()+"Z"
        self.pre_stats = _proc_stats()
        self.env = _env_snapshot()
        self.phases = []  # list of {"name","t0","t1","secs","error"}
        self.kv = {}      # custom metrics/fields
        self.http = _HTTPProbe()
        if http_probe: self.http.enable()

    # ---- section timing ----
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

    # ---- global capture of stdout/stderr ----
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

    # ---- misc helpers ----
    def add_meta(self, **kwargs):
        self.kv.update(kwargs or {})

    def _write(self, s: str):
        try: self.buf.write(s)
        except Exception: pass

    # ---- dump files ----
    def dump(self) -> str:
        os.makedirs(LOG_DIR, exist_ok=True)
        os.makedirs(MET_DIR, exist_ok=True)

        # post stats & leak heuristics
        post_stats = _proc_stats()
        try:
            self.http.disable()
        except Exception:
            pass

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
            "http_calls": http_calls[:200],  # cap
            "meta": self.kv,
        }

        # write manifest json
        manifest_path = os.path.join(MET_DIR, f"run_manifest_{self.run_id}.json")
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)

        # append to run_history
        hist_path = os.path.join(MET_DIR, "run_history.json")
        try:
            hist = json.load(open(hist_path)) if os.path.exists(hist_path) else []
        except Exception:
            hist = []
        hist.append({k: summary[k] for k in ("run_id","label","started_utc","ended_utc","errors_count","leak_flags")})
        hist = hist[-200:]
        with open(hist_path, "w", encoding="utf-8") as f:
            json.dump(hist, f, indent=2)

        # write redacted log
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

        # write errors-only convenience file (for quick triage)
        if self.errors:
            err_path = os.path.join(LOG_DIR, f"errors_only_{self.run_id}.txt")
            with open(err_path, "w", encoding="utf-8") as f:
                for i, e in enumerate(self.errors, 1):
                    f.write(f"[{i}] section={e.get('section')} err={e.get('error')}\n")
                    f.write(_redact(e.get("traceback","")) + "\n")

        return log_path
