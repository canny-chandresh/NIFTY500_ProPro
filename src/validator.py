# src/validator.py
from __future__ import annotations
import pandas as pd

REQ_OHLCV = ["Symbol","DateTime","Open","High","Low","Close","Volume"]
REQ_ORDERS = ["when_utc","engine","mode","Symbol","Entry","Target","SL","proba","status"]

def validate_ohlcv(df: pd.DataFrame) -> tuple[bool, list[str]]:
    miss = [c for c in REQ_OHLCV if c not in df.columns]
    return (len(miss) == 0, miss)

def validate_orders_df(df: pd.DataFrame) -> tuple[bool, list[str]]:
    miss = [c for c in REQ_ORDERS if c not in df.columns]
    # sanity: TP>Entry>SL for long (basic check; skip if NaNs)
    bad = []
    if "Entry" in df and "Target" in df and "SL" in df:
        try:
            mask = (df["Target"] <= df["Entry"]) | (df["SL"] >= df["Entry"])
            if mask.any():
                bad.append(f"{int(mask.sum())} rows violate (TP>Entry>SL)")
        except Exception:
            pass
    return (len(miss) == 0 and not bad, miss + bad)
