# -*- coding: utf-8 -*-
"""
telegram.py
Hardened Telegram sender with length splitting and HTML fallbacks.
Reads TG_BOT_TOKEN and TG_CHAT_ID from environment.
"""

from __future__ import annotations
import os, time, json, html
import requests

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "")

MAX_LEN = 3900  # leave room for formatting

def _post(payload, parse="HTML"):
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        print("[telegram] missing TG_BOT_TOKEN/TG_CHAT_ID; skipping send.")
        return {"status": "skipped"}
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, data={
            "chat_id": TG_CHAT_ID,
            "text": payload,
            "parse_mode": parse
        }, timeout=20)
        if r.status_code != 200:
            print("[telegram] HTTP", r.status_code, r.text[:200])
        return {"status": r.status_code, "resp": r.text[:200]}
    except Exception as e:
        print("[telegram] EXC:", e)
        return {"status": "error", "error": repr(e)}

def _chunks(txt: str, max_len=MAX_LEN):
    while txt:
        yield txt[:max_len]
        txt = txt[max_len:]

def _send_text(txt: str, html_mode=True):
    """Split & send; downgrade to plain on 400."""
    mode = "HTML" if html_mode else "MarkdownV2"
    for part in _chunks(txt):
        resp = _post(part, parse=mode)
        if resp.get("status") == 400 and html_mode:
            # retry as plain text
            safe = html.unescape(part)
            _post(safe, parse="")  # no parse mode
        time.sleep(0.3)

def _format_recos(title: str, picks: list[str], footer: str | None = None) -> str:
    body = [f"<b>{html.escape(title)}</b>"]
    body += [html.escape(p) for p in picks]
    if footer:
        body.append(html.escape(footer))
    return "\n".join(body)

# ---- Public helpers ----

def send_recommendations(title: str, lines: list[str], footer: str | None = None):
    msg = _format_recos(title, lines, footer)
    _send_text(msg, html_mode=True)

def _send(text: str, html: bool = True):
    if html:
        _send_text(text, html_mode=True)
    else:
        _send_text(text, html_mode=False)
