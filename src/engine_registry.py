# src/engine_registry.py
from typing import Callable, Dict, Any

_REG: Dict[str, Dict[str, Callable[..., Any]]] = {}

def register_engine(name: str, train_fn: Callable, predict_fn: Callable):
    _REG[name] = {"train": train_fn, "predict": predict_fn}

def get_engine(name: str):
    return _REG.get(name)

def list_engines():
    return list(_REG.keys())
