# src/automl_tuner.py
from __future__ import annotations
import json, math
from pathlib import Path
from typing import Dict, Tuple

import numpy as np
import pandas as pd

try:
    import optuna
    OPTUNA_OK = True
except Exception:
    OPTUNA_OK = False

# Simple baseline model (scikit)
try:
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.model_selection import StratifiedKFold
    from sklearn.metrics import roc_auc_score
    SK_OK = True
except Exception:
    SK_OK = False

from _engine_utils import feature_cols

def _paths(cfg: Dict) -> Tuple[Path, Path]:
    rep = Path(cfg.get("paths", {}).get("reports", "reports"))
    out = rep / "automl"
    out.mkdir(parents=True, exist_ok=True)
    return rep, out

def _prep(train_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Series]:
    X = train_df[feature_cols(train_df)].replace([np.inf, -np.inf], np.nan).fillna(0.0)
    y = (train_df.get("y_1d", pd.Series(0, index=train_df.index)) > 0).astype(int)
    return X, y

def _cv_score(X: pd.DataFrame, y: pd.Series, params: Dict, n_splits: int = 3) -> float:
    if not SK_OK:  # no sklearn available
        return 0.5
    cv = StratifiedKFold(n_splits=n_splits, shuffle=True, random_state=42)
    scores = []
    for tr, va in cv.split(X, y):
        clf = RandomForestClassifier(
            n_estimators=int(params.get("n_estimators", 150)),
            max_depth=int(params.get("max_depth", 6)),
            min_samples_leaf=int(params.get("min_samples_leaf", 1)),
            random_state=42,
            n_jobs=-1,
        )
        clf.fit(X.iloc[tr], y.iloc[tr])
        try:
            p = clf.predict_proba(X.iloc[va])[:, 1]
            s = roc_auc_score(y.iloc[va], p)
        except Exception:
            s = 0.5
        scores.append(s)
    return float(np.mean(scores)) if scores else 0.5

def _optuna_search(X: pd.DataFrame, y: pd.Series, trials: int = 20) -> Tuple[Dict, float, pd.DataFrame]:
    def objective(trial):
        params = {
            "n_estimators": trial.suggest_int("n_estimators", 80, 300),
            "max_depth": trial.suggest_int("max_depth", 3, 12),
            "min_samples_leaf": trial.suggest_int("min_samples_leaf", 1, 8),
        }
        return _cv_score(X, y, params)

    study = optuna.create_study(direction="maximize")
    study.optimize(objective, n_trials=trials, show_progress_bar=False)
    hist = pd.DataFrame({"trial": [t.number for t in study.trials],
                         "value": [t.value for t in study.trials],
                         "params": [t.params for t in study.trials]}).sort_values("value", ascending=False)
    return study.best_params, float(study.best_value), hist

def _grid_search(X: pd.DataFrame, y: pd.Series) -> Tuple[Dict, float, pd.DataFrame]:
    grid = []
    for ne in (120, 180, 250):
        for md in (4, 6, 8, 10):
            for msl in (1, 2, 4):
                params = {"n_estimators": ne, "max_depth": md, "min_samples_leaf": msl}
                score = _cv_score(X, y, params)
                grid.append((params, score))
    grid.sort(key=lambda x: x[1], reverse=True)
    hist = pd.DataFrame([{"params": p, "value": s} for p, s in grid])
    return grid[0][0], float(grid[0][1]), hist

def run_automl(train_df: pd.DataFrame, cfg: Dict, tag: str = "ml") -> Dict:
    rep, out = _paths(cfg)
    if train_df is None or train_df.empty:
        return {"ok": False, "error": "empty_train"}

    X, y = _prep(train_df)
    if y.sum() == 0 or y.sum() == len(y):
        # degenerate target; bail
        best_params, best_score, hist = {"degenerate": True}, 0.5, pd.DataFrame()
    else:
        if OPTUNA_OK and SK_OK and bool(cfg.get("automl", {}).get("enable", True)):
            trials = int(cfg.get("automl", {}).get("max_trials_per_bucket", 20))
            best_params, best_score, hist = _optuna_search(X, y, trials=trials)
        else:
            best_params, best_score, hist = _grid_search(X, y)

    # Persist artifacts
    (out / f"leaderboard_{tag}.csv").write_text(
        hist.to_csv(index=False) if not hist.empty else "params,value\n", encoding="utf-8"
    )
    (out / f"best_{tag}.json").write_text(
        json.dumps({"best_params": best_params, "best_score": best_score}, indent=2), encoding="utf-8"
    )

    return {"ok": True, "best_params": best_params, "best_score": best_score, "rows": int(len(hist))}
