# -*- coding: utf-8 -*-
"""
News extractor (stub). Add RSS/API URLs in CONFIG['discovery']['sources']['news'] to activate.
Writes datalake/discovery/raw/news/news.jsonl
"""

from __future__ import annotations
import json, time
from pathlib import Path
from typing import List
from config import CONFIG

RAW = Path(CONFIG["paths"]["datalake"]) / "discovery" / "raw" / "news"
RAW.mkdir(parents=True, exist_ok=True)

def fetch() -> str:
    urls: List[str] = CONFIG.get("discovery",{}).get("sources",{}).get("news", [])
    fp = RAW/"news.jsonl"
    if not urls:
        fp.write_text("")  # empty heartbeat
        return str(fp)
    # Minimal fetcher placeholder (no external libs here)
    # You can replace with feedparser/requests as needed.
    with fp.open("a", encoding="utf-8") as f:
        f.write(json.dumps({"ts": time.time(), "info": "stub - add real feeds"}) + "\n")
    return str(fp)
