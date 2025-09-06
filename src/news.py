# src/news.py
from __future__ import annotations
import os, re, csv, json, time, hashlib, datetime as dt
from pathlib import Path

import requests

NEWS_DIR = Path("datalake")
NEWS_CSV = NEWS_DIR / "news_sentiment.csv"
STATE_JSON = NEWS_DIR / "news_state.json"
NEWS_DIR.mkdir(parents=True, exist_ok=True)

# Free sources via RSS / web (stay polite; low frequency)
RSS_FEEDS = [
    "https://news.google.com/rss/search?q=NIFTY50",
    "https://news.google.com/rss/search?q=NSE+stocks",
    "https://www.moneycontrol.com/rss/MCtopnews.xml",
    "https://www.moneycontrol.com/rss/marketreports.xml",
]
HEADERS = {"User-Agent": "NIFTY500-ProPro/1.0 (+https://github.com/)"}
TIMEOUT = 10

def _hash(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()

def _sentiment_heuristic(title: str) -> str:
    t = title.lower()
    pos = any(k in t for k in ["surge","rally","beat","upgrade","profit","growth","wins"])
    neg = any(k in t for k in ["slash","downgrade","loss","fraud","probe","plunge","fall","miss"])
    if pos and not neg: return "positive"
    if neg and not pos: return "negative"
    return "neutral"

def _load_state() -> dict:
    if STATE_JSON.exists():
        try: return json.load(open(STATE_JSON))
        except Exception: pass
    return {"seen": {}}

def _save_state(s: dict):
    json.dump(s, open(STATE_JSON,"w"), indent=2)

def fetch_and_update(max_items: int = 200):
    """
    Fetch news titles; dedupe by hash; score simple sentiment; append CSV.
    """
    state = _load_state()
    seen = state.get("seen", {})

    rows = []
    for url in RSS_FEEDS:
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            if r.status_code != 200: continue
            # simple RSS title extraction
            titles = re.findall(r"<title><!\[CDATA\[(.*?)\]\]></title>", r.text)
            # drop the feed title itself (first item)
            if titles: titles = titles[1:]
            for t in titles[:max_items//len(RSS_FEEDS)]:
                h = _hash(t)
                if h in seen: continue
                sent = _sentiment_heuristic(t)
                rows.append({
                    "when_utc": dt.datetime.utcnow().isoformat()+"Z",
                    "source": url,
                    "title": t.strip(),
                    "hash": h,
                    "sentiment": sent
                })
                seen[h] = int(time.time())
        except Exception:
            continue

    if not rows:
        return {"added": 0, "total_seen": len(seen)}

    write_header = not NEWS_CSV.exists()
    with open(NEWS_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["when_utc","source","title","hash","sentiment"])
        if write_header: w.writeheader()
        w.writerows(rows)

    state["seen"] = seen
    _save_state(state)
    return {"added": len(rows), "total_seen": len(seen)}
