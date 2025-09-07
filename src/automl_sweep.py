# src/automl_sweep.py
from __future__ import annotations
import json, datetime as dt
from pathlib import Path
import numpy as np, pandas as pd
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import log_loss
from xgboost import XGBClassifier

REP = Path("reports/champion"); REP.mkdir(parents=True, exist_ok=True)

def _load_matrix(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, parse_dates=["Date"])

def sweep_feature_matrix(matrix_path: str, target="y_1d") -> dict:
    df = _load_matrix(Path(matrix_path))
    df = df.dropna().reset_index(drop=True)
    if target not in df.columns: return {"ok": False, "reason": "no_target"}
    y = (df[target] > 0).astype(int)
    X = df.drop(columns=["Date", target])
    tscv = TimeSeriesSplit(n_splits=5)
    param_grid = [
        {"max_depth":3,"n_estimators":200,"learning_rate":0.05,"subsample":0.8,"colsample_bytree":0.8},
        {"max_depth":4,"n_estimators":300,"learning_rate":0.03,"subsample":0.9,"colsample_bytree":0.8},
        {"max_depth":5,"n_estimators":400,"learning_rate":0.025,"subsample":0.8,"colsample_bytree":0.7},
    ]
    results=[]
    for p in param_grid:
        ll=[]
        for tr, te in tscv.split(X):
            m = XGBClassifier(**p, random_state=42, n_jobs=2, eval_metric="logloss")
            m.fit(X.iloc[tr], y.iloc[tr])
            proba = m.predict_proba(X.iloc[te])[:,1]
            ll.append(log_loss(y.iloc[te], proba, labels=[0,1]))
        results.append({"params":p,"logloss":float(np.mean(ll))})
    out = {"when_utc": dt.datetime.utcnow().isoformat()+"Z", "results":sorted(results, key=lambda r:r["logloss"])}
    (REP / "sweep_results.json").write_text(json.dumps(out, indent=2), encoding="utf-8")
    return out
