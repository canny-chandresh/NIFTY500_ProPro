# src/news_ingest.py
"""
Lightweight news ingestion with multiple fallbacks:
- Tries RSS endpoints (no keys; polite).
- Falls back to a local "seed_news.csv" if network fails.
Outputs:
- datalake/news/news_<YYYYMMDD_HH>.json  (raw items)
- datalake/news/news_latest.json         (symlink/copy for latest)
Downstream:
- sentiment.py scores to per-item sentiment.
"""

from __future__ import annotations
from pathlib import Path
from typing import List, Dict
import json, datetime as dt
import re

import pandas as pd

try:
    import feedparser  # optional but nice to have
except Exception:
    feedparser = None

DL = Path("datalake")
NEWS_DIR = DL / "news"
NEWS_DIR.mkdir(parents=True, exist_ok=True)

# A few finance RSS candidates; you can add more or limit per locale
RSS_LIST = [
    "https://www.moneycontrol.com/rss/latestnews.xml",
    "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
    "https://www.livemint.com/rss/markets",
]

def _clean_text(x: str) -> str:
    if not isinstance(x, str): return ""
    x = re.sub(r"<[^>]+>", " ", x)
    x = re.sub(r"\s+", " ", x).strip()
    return x

def fetch_news(max_items: int = 80) -> List[Dict]:
    items: List[Dict] = []
    if feedparser is not None:
        for url in RSS_LIST:
            try:
                d = feedparser.parse(url)
                for e in d.entries[:max_items//len(RSS_LIST) + 1]:
                    items.append({
                        "source": d.feed.get("title", "rss"),
                        "title": _clean_text(e.get("title")),
                        "summary": _clean_text(e.get("summary", "")),
                        "link": e.get("link", ""),
                        "published": e.get("published", ""),
                    })
            except Exception:
                continue

    # Fallback to seed file if empty
    if not items:
        seed = DL / "seed_news.csv"
        if seed.exists():
            df = pd.read_csv(seed).head(max_items)
            for _, r in df.iterrows():
                items.append({
                    "source": r.get("source", "seed"),
                    "title": _clean_text(r.get("title", "")),
                    "summary": _clean_text(r.get("summary", "")),
                    "link": r.get("link", ""),
                    "published": r.get("published", ""),
                })
    return items

def write_news_bundle(items: List[Dict]) -> Dict:
    ts = dt.datetime.utcnow().strftime("%Y%m%d_%H")
    path = NEWS_DIR / f"news_{ts}.json"
    path.write_text(json.dumps({"when_utc": dt.datetime.utcnow().isoformat()+"Z", "items": items}, indent=2), encoding="utf-8")
    # update latest pointer
    latest = NEWS_DIR / "news_latest.json"
    latest.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
    return {"ok": True, "path": str(path), "latest": str(latest), "count": len(items)}

if __name__ == "__main__":
    b = write_news_bundle(fetch_news())
    print(b)
