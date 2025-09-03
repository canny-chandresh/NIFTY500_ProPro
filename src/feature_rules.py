# src/feature_rules.py
import pandas as pd
from indicators import ema

def add_basic_rules(df_sym: pd.DataFrame):
    if df_sym.empty: return df_sym
    df = df_sym.copy()
    df["EMA20"]  = ema(df["Close"], 20)
    df["EMA200"] = ema(df["Close"], 200)
    prev = df.shift(1)
    pp = (prev["High"] + prev["Low"] + prev["Close"]) / 3
    r1 = 2*pp - prev["Low"]
    s1 = 2*pp - prev["High"]
    df["PP"], df["R1"], df["S1"] = pp, r1, s1
    df["gap_up"] = (df["Open"] > prev["High"]).astype(int)
    df["gap_down"] = (df["Open"] < prev["Low"]).astype(int)
    df["res_200"] = (df["Close"] < df["EMA200"]).astype(int)
    df["sup_200"] = (df["Close"] > df["EMA200"]).astype(int)
    df["res_20"]  = (df["Close"] < df["EMA20"]).astype(int)
    df["sup_20"]  = (df["Close"] > df["EMA20"]).astype(int)
    return df

def reason_from_rules(latest_row: pd.Series)->str:
    if latest_row.get("gap_up",0)==1:
        return "Gap-up; watch fill risk"
    if latest_row.get("gap_down",0)==1:
        return "Gap-down; possible bounce"
    if latest_row.get("sup_200",0)==1 and latest_row.get("sup_20",0)==1:
        return "Above 20/200EMA (trend support)"
    if latest_row.get("res_200",0)==1:
        return "Below 200EMA (resistance)"
    c = latest_row.get("Close",0)
    if c > latest_row.get("R1",1e12):   return "Near/above R1"
    if c < latest_row.get("S1",-1e12):  return "Near/below S1"
    return "Technical mixed"
