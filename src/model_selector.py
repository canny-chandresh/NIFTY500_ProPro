# src/model_selector.py
"""
Model selector/orchestrator for NIFTY500_ProPro.

Responsibilities
- Import and register all engine wrappers (ML/DL/Algo/Auto/UFD).
- Run the active engines on (train_df, pred_df).
- Return a standardized predictions DataFrame with columns:
    ["symbol", "Score", "WinProb", "Reason", "engine"]
- Be defensive: if an engine fails to import or run, continue with others.

Notes
- Blending (rank/score aggregation) and portfolio caps are handled in pipeline.py.
- Engines self-register via engine_registry.register_engine(...) when imported.
"""

from __future__ import annotations
import importlib
from pathlib import Path
from typing import Dict, List

import pandas as pd

# --- Optional config (engines_active default) ---
try:
    import config
    _CFG = getattr(config, "CONFIG", {})
    _ENGINES_ACTIVE_DEFAULT = _CFG.get("engines_active", [])
except Exception:
    _CFG = {}
    _ENGINES_ACTIVE_DEFAULT = [
        "ML_ROBUST", "ALGO_RULES", "AUTO_TOPK", "UFD_PROMOTED",
        "DL_TEMPORAL", "DL_TRANSFORMER", "DL_GNN"
    ]

# --- Ensure engines are loaded so they self-register ---
# Add/remove modules here if you create new engines.
_ENGINE_MODULES: List[str] = [
    "engine_ml_robust",
    "engine_algo_rules",
    "engine_auto",
    "engine_ufd",
    "engine_dl_temporal",
    "engine_dl_transformer",
    "engine_gnn",
    "engine_lstm",              # optional baseline; will fail safe if missing deps
]

def _import_engines():
    for mod in _ENGINE_MODULES:
        try:
            importlib.import_module(mod)
        except Exception as e:
            print(f"[model_selector] optional engine '{mod}' not loaded: {e}")

_import_engines()

# --- Registry runner ---
from engine_registry import run_engine, list_engines

# --- Public API ---

def run_engines(train_df: pd.DataFrame,
                pred_df: pd.DataFrame,
                cfg: Dict | None = None) -> pd.DataFrame:
    """
    Execute all active engines on the given (train, pred) split.

    Parameters
    ----------
    train_df : pd.DataFrame
        Historical rows per symbol (>= 1 row/symbol recommended).
        Should include target column y_1d for supervised engines.
    pred_df : pd.DataFrame
        Latest row per symbol to score (1 row per symbol ideally).
    cfg : Dict | None
        Configuration dict. If None, tries config.CONFIG fallback.

    Returns
    -------
    pd.DataFrame
        Concatenated predictions with columns:
        ["symbol", "Score", "WinProb", "Reason", "engine"]
        May be empty if no engine produced output.
    """
    if cfg is None:
        cfg = _CFG

    active: List[str] = cfg.get("engines_active") or _ENGINES_ACTIVE_DEFAULT
    if not isinstance(active, list) or not active:
        print("[model_selector] No active engines configured.")
        return _empty_pred_df()

    frames: List[pd.DataFrame] = []
    for name in active:
        try:
            df = run_engine(name, train_df, pred_df, cfg)
            if not isinstance(df, pd.DataFrame) or df.empty:
                continue
            # Standardize columns if engine missed any optional ones
            for col in ["symbol", "Score", "WinProb", "Reason", "engine"]:
                if col not in df.columns:
                    if col == "symbol" and "Symbol" in df.columns:
                        df["symbol"] = df["Symbol"]
                    elif col in ("Score", "WinProb"):
                        df[col] = 0.0
                    elif col in ("Reason", "engine"):
                        df[col] = name if col == "engine" else ""
            # Keep only standard columns (plus any extras the engine added)
            df = df[["symbol", "Score", "WinProb", "Reason", "engine"] + [c for c in df.columns
                                                                           if c not in {"symbol","Score","WinProb","Reason","engine"}]]
            frames.append(df)
        except Exception as e:
            print(f"[model_selector] engine {name} failed during run: {e}")

    if not frames:
        return _empty_pred_df()

    out = pd.concat(frames, ignore_index=True)
    # Basic sanitation
    try:
        out["Score"] = pd.to_numeric(out["Score"], errors="coerce").fillna(0.0)
        out["WinProb"] = pd.to_numeric(out["WinProb"], errors="coerce").clip(0.05, 0.95).fillna(0.5)
    except Exception:
        pass
    return out


def engines_available() -> List[str]:
    """
    List currently registered engines (after imports).
    """
    try:
        return list_engines()
    except Exception:
        return []


# --- Helpers ---

def _empty_pred_df() -> pd.DataFrame:
    return pd.DataFrame(columns=["symbol","Score","WinProb","Reason","engine"])
