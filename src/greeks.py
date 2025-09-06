# src/greeks.py
from __future__ import annotations
import math
import pandas as pd

def _phi(x):  # standard normal pdf
    return (1.0/math.sqrt(2*math.pi))*math.exp(-0.5*x*x)

def _Phi(x):  # cdf
    return 0.5*(1.0+math.erf(x/math.sqrt(2)))

def black_scholes_greeks(S, K, T, r, sigma, call=True):
    if sigma<=0 or T<=0:  # safe defaults
        return {"delta":0.0,"gamma":0.0,"vega":0.0,"theta":0.0,"rho":0.0}
    d1 = (math.log(S/K) + (r + 0.5*sigma*sigma)*T) / (sigma*math.sqrt(T))
    d2 = d1 - sigma*math.sqrt(T)
    if call:
        delta = _Phi(d1)
        theta = (-(S*_phi(d1)*sigma)/(2*math.sqrt(T)) - r*K*math.exp(-r*T)*_Phi(d2))
        rho   = K*T*math.exp(-r*T)*_Phi(d2)
    else:
        delta = _Phi(d1)-1.0
        theta = (-(S*_phi(d1)*sigma)/(2*math.sqrt(T)) + r*K*math.exp(-r*T)*_Phi(-d2))
        rho   = -K*T*math.exp(-r*T)*_Phi(-d2)
    gamma = _phi(d1)/(S*sigma*math.sqrt(T))
    vega  = S*_phi(d1)*math.sqrt(T)
    return {"delta":delta,"gamma":gamma,"vega":vega,"theta":theta,"rho":rho}

def implied_vol_newton(S, K, T, r, price, call=True, init=0.2, tol=1e-5, max_iter=50):
    sigma = max(1e-4, float(init))
    for _ in range(max_iter):
        # price under current sigma
        d1 = (math.log(S/K) + (r + 0.5*sigma*sigma)*T) / (sigma*math.sqrt(T))
        d2 = d1 - sigma*math.sqrt(T)
        if call:
            theo = S*_Phi(d1) - K*math.exp(-r*T)*_Phi(d2)
        else:
            theo = K*math.exp(-r*T)*_Phi(-d2) - S*_Phi(-d1)
        vega = S*_phi(d1)*math.sqrt(T)
        diff = theo - price
        if abs(diff) < tol: break
        if vega < 1e-8: break
        sigma -= diff/vega
        if sigma<=1e-6: sigma = 1e-6
    return float(sigma)

def compute_chain_greeks(df: pd.DataFrame, r: float = 0.06, S_col="UNDERLYING", t_col="DTE", price_col="LTP"):
    """
    df must include: STRIKE, TYPE ('CE'/'PE'), DTE (in years), LTP, UNDERLYING
    Adds: IV, delta, gamma, vega, theta, rho
    """
    out = df.copy()
    ivs = []; dels=[]; gms=[]; vgs=[]; ths=[]; rhs=[]
    for _, r0 in out.iterrows():
        S = float(r0.get(S_col, 0.0)); K = float(r0["STRIKE"]); T = float(r0[t_col]); call = str(r0["TYPE"]).upper()=="CE"
        px = float(r0.get(price_col, 0.0))
        iv = implied_vol_newton(S, K, T, r, px, call=call, init=0.2)
        g  = black_scholes_greeks(S, K, T, r, iv, call=call)
        ivs.append(iv); dels.append(g["delta"]); gms.append(g["gamma"]); vgs.append(g["vega"]); ths.append(g["theta"]); rhs.append(g["rho"])
    out["IV"]=ivs; out["delta"]=dels; out["gamma"]=gms; out["vega"]=vgs; out["theta"]=ths; out["rho"]=rhs
    return out
