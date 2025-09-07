# src/model_selector.py
from engine_registry import get_engine
import pandas as pd

def run_engines(train_df, pred_df, cfg):
    names = cfg.get("engines_active",["ML_ROBUST","DL_TEMPORAL","ALGO_RULES","AUTO_TOPK","UFD_PROMOTED"])
    outputs=[]
    for name in names:
        eng=get_engine(name)
        if not eng: continue
        model=eng["train"](train_df,cfg)
        out=eng["predict"](model,pred_df,cfg)
        out["engine"]=name
        outputs.append(out)
    return pd.concat(outputs,ignore_index=True)
