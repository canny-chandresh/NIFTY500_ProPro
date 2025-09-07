"""
Shadow Graph Neural Network engine.
"""

import pandas as pd
from engine_registry import register_engine

def train_gnn(df, cfg): return {"gnn":"dummy"}

def predict_gnn(model, df, cfg):
    out = df[["symbol"]].copy()
    out["Score"] = df.get("GRAPH_deg", pd.Series(0)).fillna(0)
    out["WinProb"] = 0.55
    out["Reason"] = "GNN shadow"
    return out

register_engine("DL_GNN", train_gnn, predict_gnn)
