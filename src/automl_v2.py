# src/automl_v2.py
from __future__ import annotations
import json
from pathlib import Path
from typing import Dict, Tuple

import numpy as np
import pandas as pd

try:
    import optuna
    OPTUNA_OK = True
except Exception:
    OPTUNA_OK = False

# Try multiple learners
LEARNERS = {}

try:
    import lightgbm as lgb
    LEARNERS["lgbm"] = "lgbm"
except Exception:
    pass

try:
    from xgboost import XGBClassifier
    LEARNERS["xgb"] = "xgb"
except Exception:
    pass

try:
    from sklearn.ensemble import RandomForestClassifier
    LEARNERS["rf"] = "rf"
    from sklearn.model_selection import StratifiedKFold
    from sklearn.metrics import roc_auc_score
    SK_OK = True
except Exception:
    SK_OK = False

from _engine_utils import feature_cols

def _paths(cfg: Dict):
    rep = Path(cfg.get("paths", {}).get("reports", "reports"))
    out = rep / "automl_v2"
    out.mkdir(parents=True, exist_ok=True)
    return rep, out

def _prep(train_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Series]:
    X = train_df[feature_cols(train_df)].replace([np.inf,-np.inf], np.nan).fillna(0.0)
    y = (train_df.get("y_1d", pd.Series(0, index=train_df.index)) > 0).astype(int)
    return X, y

def _cv_score(model_name: str, X: pd.DataFrame, y: pd.Series, params: Dict, n_splits=3) -> float:
    if not SK_OK: return 0.5
    cv = StratifiedKFold(n_splits=n_splits, shuffle=True, random_state=42)
    scores = []
    for tr, va in cv.split(X, y):
        if model_name == "rf":
            clf = RandomForestClassifier(
                n_estimators=int(params.get("n_estimators",160)),
                max_depth=int(params.get("max_depth",8)),
                min_samples_leaf=int(params.get("min_samples_leaf",2)),
                random_state=42, n_jobs=-1
            )
            clf.fit(X.iloc[tr], y.iloc[tr])
            p = clf.predict_proba(X.iloc[va])[:,1]
        elif model_name == "xgb" and "xgb" in LEARNERS:
            clf = XGBClassifier(
                n_estimators=int(params.get("n_estimators",200)),
                max_depth=int(params.get("max_depth",5)),
                learning_rate=float(params.get("lr",0.07)),
                subsample=0.9, colsample_bytree=0.9,
                objective="binary:logistic", eval_metric="auc",
                n_jobs=-1, tree_method="hist"
            )
            clf.fit(X.iloc[tr], y.iloc[tr])
            p = clf.predict_proba(X.iloc[va])[:,1]
        elif model_name == "lgbm" and "lgbm" in LEARNERS:
            clf = lgb.LGBMClassifier(
                n_estimators=int(params.get("n_estimators",250)),
                max_depth=int(params.get("max_depth",-1)),
                learning_rate=float(params.get("lr",0.05)),
                subsample=0.9, colsample_bytree=0.9,
                objective="binary"
            )
            clf.fit(X.iloc[tr], y.iloc[tr])
            p = clf.predict_proba(X.iloc[va])[:,1]
        else:
            return 0.5
        try:
            s = roc_auc_score(y.iloc[va], p)
        except Exception:
            s = 0.5
        scores.append(s)
    return float(np.mean(scores)) if scores else 0.5

def run(train_df: pd.DataFrame, cfg: Dict) -> Dict:
    rep, out = _paths(cfg)
    if train_df is None or train_df.empty:
        return {"ok": False, "error": "empty_train"}

    X, y = _prep(train_df)
    if y.nunique() < 2:
        return {"ok": False, "error": "degenerate_target"}

    learners = [m for m in ["lgbm","xgb","rf"] if m in LEARNERS]
    trials   = int(cfg.get("automl", {}).get("max_trials_per_bucket", 30))
    results  = []

    def suggest(trial, model):
        if model=="rf":
            return {
                "n_estimators": trial.suggest_int("n_estimators", 120, 300),
                "max_depth": trial.suggest_int("max_depth", 4, 12),
                "min_samples_leaf": trial.suggest_int("min_samples_leaf", 1, 5),
            }
        else:
            return {
                "n_estimators": trial.suggest_int("n_estimators", 120, 400),
                "max_depth": trial.suggest_int("max_depth", 3, 10),
                "lr": trial.suggest_float("lr", 0.01, 0.2),
            }

    if OPTUNA_OK and learners:
        for model in learners:
            study = optuna.create_study(direction="maximize")
            def objective(trial):
                ps = suggest(trial, model)
                return _cv_score(model, X, y, ps)
            study.optimize(objective, n_trials=trials, show_progress_bar=False)
            results.append({"model": model, "best_value": float(study.best_value), "best_params": study.best_params})
    else:
        # tiny manual sweep
        for model in learners:
            grid = [{"n_estimators": n, "max_depth": d} for n in (150,220,280) for d in (4,6,8)]
            best = {"model": model, "best_value": -1, "best_params": None}
            for g in grid:
                sc = _cv_score(model, X, y, g)
                if sc > best["best_value"]:
                    best["best_value"] = sc
                    best["best_params"] = g
            results.append(best)

    res_df = pd.DataFrame(results).sort_values("best_value", ascending=False)
    res_df.to_csv(out / "leaderboard_models.csv", index=False)
    if not res_df.empty:
        best = res_df.iloc[0].to_dict()
    else:
        best = {"model": None, "best_value": 0.5, "best_params": {}}
    (out / "best_model.json").write_text(json.dumps(best, indent=2), encoding="utf-8")
    return {"ok": True, "count": int(len(res_df)), "best": best}
