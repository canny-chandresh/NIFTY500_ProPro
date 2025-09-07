# src/engine_registry.py
from __future__ import annotations
from typing import Callable, Dict, Any

_REG: Dict[str, Dict[str, Callable[..., Any]]] = {
    # name -> {"train": fn, "predict": fn}
}

def register_engine(name: str, train_fn: Callable, predict_fn: Callable):
    _REG[name] = {"train": train_fn, "predict": predict_fn}

def get_engine(name: str):
    return _REG.get(name)

def list_engines() -> Dict[str, Dict[str, Callable]]:
    return dict(_REG)
