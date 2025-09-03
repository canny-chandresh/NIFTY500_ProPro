# src/indicators.py
import pandas as pd, numpy as np

def ema(s: pd.Series, span: int):
    return s.ewm(span=span, adjust=False).mean()

def sma(s: pd.Series, n: int):
    return s.rolling(n, min_periods=max(2, n//3)).mean()

def rsi(close: pd.Series, n: int = 14):
    delta = close.diff()
    up = (delta.clip(lower=0)).rolling(n).mean()
    down = (-delta.clip(upper=0)).rolling(n).mean()
    rs = up / (down.replace(0, np.nan))
    return 100 - (100 / (1 + rs))

def pct_above_sma(close: pd.Series, n: int = 50):
    ma = sma(close, n)
    return (close > ma).rolling(n).mean()
