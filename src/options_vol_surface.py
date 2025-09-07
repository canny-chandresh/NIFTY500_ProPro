"""
Vol surface builder (SVI placeholder).
"""

import pandas as pd, numpy as np

def fit_vol_surface(df: pd.DataFrame):
    # dummy SVI fit: just quadratic
    strikes, iv = df["strike"], df["iv"]
    coeffs = np.polyfit(strikes, iv, 2)
    return coeffs

def implied_vol(coeffs, strike):
    return np.polyval(coeffs, strike)
