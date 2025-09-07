"""
Time-of-day slippage model. Adds spread penalty depending on intraday bar.
"""

import numpy as np
import pandas as pd

# Simple static curves (percent of price)
SPREAD_CURVE = {
    "open": 0.003,   # 30 bps at open
    "mid": 0.001,    # 10 bps midday
    "close": 0.002   # 20 bps near close
}

def estimate_slippage(time: pd.Timestamp, price: float) -> float:
    h = time.hour
    if h < 10: pct = SPREAD_CURVE["open"]
    elif h < 15: pct = SPREAD_CURVE["mid"]
    else: pct = SPREAD_CURVE["close"]
    return price * pct

def apply_slippage(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["slippage"] = df.apply(lambda r: estimate_slippage(r["Date"], r["Price"]), axis=1)
    df["price_adj"] = df["Price"] - df["slippage"]
    return df
