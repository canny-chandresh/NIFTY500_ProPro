from __future__ import annotations
import os, time, json, datetime as dt
from typing import List, Dict
import re
import xml.etree.ElementTree as ET
import urllib.request

def _fetch_rss(url: str, timeout: int = 12) -> List[Dict]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            xml = r.read()
        root = ET.fromstring(xml)
        items = []
        for it in root.iterfind(".//item"):
            title = (it.findtext("title") or "").strip()
            desc  = (it.findtext("description") or "").strip()
            pub   = (it.findtext("pubDate") or "").strip()
            items.append({"title":title, "desc":desc, "pubDate":pub})
        return items
    except Exception:
        return []

def _match_score(text: str, pos: List[str], neg: List[str]) -> int:
    t = text.lower()
    score = 0
    for w in pos:
        if re.search(r"\b" + re.escape(w.lower()) + r"\b", t): score += 1
    for w in neg:
        if re.search(r"\b" + re.escape(w.lower()) + r"\b", t): score -= 1
    return score

def pulse(feeds: List[str], pos: List[str], neg: List[str], lookback_hours: int = 6) -> Dict:
    now = dt.datetime.utcnow()
    hits_pos, hits_neg = 0, 0
    sample = []
    for url in feeds:
        for it in _fetch_rss(url):
            txt = f"{it.get('title','')} {it.get('desc','')}"
            s = _match_score(txt, pos, neg)
            if s != 0:
                sample.append({"title": it.get("title","")[:120], "score": s})
                if s > 0: hits_pos += 1
                if s < 0: hits_neg += 1
    return {
        "when_utc": now.isoformat()+"Z",
        "hits_positive": hits_pos,
        "hits_negative": hits_neg,
        "samples": sample[:10]
    }

def write_pulse_report(config: dict) -> Dict:
    if not config.get("enabled", True):
        return {"enabled": False}
    data = pulse(
        config.get("feeds", []),
        config.get("keywords_positive", []),
        config.get("keywords_negative", []),
        int(config.get("lookback_hours", 6)),
    )
    os.makedirs("reports", exist_ok=True)
    with open("reports/news_pulse.json","w") as f:
        json.dump(data, f, indent=2)
    return data
