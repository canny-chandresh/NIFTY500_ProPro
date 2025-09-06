# tests/test_smoke.py
import os, json
import pandas as pd

def test_dirs():
    assert os.path.isdir("src")
    assert os.path.isdir("datalake")
    assert os.path.isdir("reports")

def test_orders_schema_if_exists():
    p = "datalake/paper_trades.csv"
    if not os.path.exists(p):
        return
    df = pd.read_csv(p, nrows=20)
    need = {"when_utc","engine","mode","Symbol","Entry","Target","SL","proba","status"}
    assert need.issubset(set(df.columns))

def test_config_loads():
    import importlib
    cfg = importlib.import_module("config").CONFIG
    assert isinstance(cfg, dict)
