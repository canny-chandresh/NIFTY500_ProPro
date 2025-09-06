from __future__ import annotations
import os, json, urllib.parse, urllib.request

def _send(msg: str):
    token = os.getenv("TG_BOT_TOKEN", "")
    chat  = os.getenv("TG_CHAT_ID", "")
    if not token or not chat: return
    base = f"https://api.telegram.org/bot{token}/sendMessage"
    data = urllib.parse.urlencode({"chat_id": chat, "text": msg}).encode()
    try:
        with urllib.request.urlopen(base, data=data, timeout=10) as r:
            r.read()
    except Exception:
        pass

def alert_if_suspended():
    try:
        p = "reports/metrics/ai_policy_suspended.json"
        if os.path.exists(p):
            j = json.load(open(p))
            if j.get("suspended"):
                _send(f"⚠️ AI policy suspended (AUTO WR={j.get('wr'):.2f}). Falling back to safe defaults.")
    except Exception:
        pass

def alert_on_run_failure(step: str, err: str):
    _send(f"❌ Step failed: {step}\n{err[:500]}")
