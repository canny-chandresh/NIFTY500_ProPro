# src/sentiment.py
"""
Scores news items:
- First tries HuggingFace transformers pipeline (if installed).
- Falls back to a tiny rule-based lexicon (no heavy deps).
Outputs a DataFrame with sentiment score in [-1, 1].
"""

from __future__ import annotations
from typing import List, Dict, Any

import numpy as np
import pandas as pd

def _try_transformers():
    try:
        from transformers import pipeline
        return pipeline("sentiment-analysis", model="distilbert-base-uncased-finetuned-sst-2-english")
    except Exception:
        return None

_HF = _try_transformers()

_POS = {"gain","rally","beat","surge","upgrade","bullish","profit","strong","buy"}
_NEG = {"fall","drop","miss","plunge","downgrade","bearish","loss","weak","sell"}

def _lexicon_score(text: str) -> float:
    t = (text or "").lower()
    pos = sum(w in t for w in _POS)
    neg = sum(w in t for w in _NEG)
    if pos == 0 and neg == 0: return 0.0
    s = (pos - neg) / (pos + neg)
    return float(max(-1.0, min(1.0, s)))

def score_items(items: List[Dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(items)
    if df.empty:
        return pd.DataFrame(columns=["title","summary","sentiment"])
    texts = (df["title"].fillna("") + ". " + df["summary"].fillna("")).tolist()

    if _HF is not None:
        try:
            out = _HF(texts, truncation=True)
            # map to [-1,1]
            sc = [ (o["score"] if o["label"].upper()=="POSITIVE" else -o["score"]) for o in out ]
            df["sentiment"] = np.array(sc, dtype=float)
            return df
        except Exception:
            pass

    # fallback
    df["sentiment"] = [ _lexicon_score(t) for t in texts ]
    return df
