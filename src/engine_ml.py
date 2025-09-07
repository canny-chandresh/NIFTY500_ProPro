# src/engine_ml.py
import pandas as pd
from engine_registry import register_engine

def train_ml(df, cfg): return {"model":"dummy"}
def predict_ml(model, df, cfg):
    out = df[["symbol"]].copy()
    out["Score"] = df["MAN_ret1"].fillna(0)
    out["WinProb"] = 0.55
    out["Reason"] = "ML baseline"
    return out

register_engine("ML_ROBUST", train_ml, predict_ml)
