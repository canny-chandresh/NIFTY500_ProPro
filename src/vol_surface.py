# src/vol_surface.py
from __future__ import annotations
import numpy as np
import pandas as pd

def fit_svi_slice(df: pd.DataFrame, m_col="moneyness", iv_col="IV") -> dict:
    """
    Slice-fit SVI: w(m)=a + b*(rho*(m-m0)+sqrt((m-m0)^2 + sigma^2))
    Works per expiry snapshot; returns parameters or quadratic fallback.
    Requires columns: moneyness (log(K/F)), IV (implied vol).
    """
    d = df.dropna(subset=[m_col, iv_col]).copy()
    if len(d) < 5:
        # fallback quadratic fit on (m, iv)
        z = np.polyfit(d[m_col], d[iv_col], 2) if len(d)>=3 else np.array([0,0,np.nanmean(d[iv_col])])
        return {"type":"quad","params":z.tolist()}
    m = d[m_col].values
    iv = d[iv_col].values
    w = iv*iv

    # crude grid search to stay dependency-free
    m0_grid = np.linspace(np.percentile(m, 25), np.percentile(m, 75), 7)
    sigma_grid = np.linspace(0.05, 0.5, 6)
    best = None; best_err = 1e99
    for m0 in m0_grid:
        for sigma in sigma_grid:
            # linear in a,b,rho given m0,sigma if we linearize sign term
            X1 = np.ones_like(m)
            X2 = np.sqrt((m - m0)**2 + sigma*sigma)
            X3 = (m - m0)
            X = np.vstack([X1, X2, X3]).T
            try:
                beta, *_ = np.linalg.lstsq(X, w, rcond=None)
                a, b, brho = beta
                b = max(1e-6, b)
                rho = np.clip(brho / b, -0.999, 0.999)
                w_hat = a + b*(rho*(m-m0) + np.sqrt((m-m0)**2 + sigma*sigma))
                err = np.mean((w_hat - w)**2)
                if err < best_err:
                    best_err = err; best = (a,b,rho,m0,sigma)
            except Exception:
                continue
    if best is None:
        z = np.polyfit(m, iv, 2)
        return {"type":"quad","params":z.tolist()}
    a,b,rho,m0,sigma = best
    return {"type":"svi","params":{"a":float(a),"b":float(b),"rho":float(rho),"m0":float(m0),"sigma":float(sigma)}}
