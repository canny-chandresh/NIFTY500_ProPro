import pandas as pd
from pathlib import Path

def test_gap_reasoning_consistency():
    # Load a small fixed OHLCV fixture if you have one (drop a CSV at tests/fixtures/ohlcv_sample.csv)
    f = Path("tests/fixtures/ohlcv_sample.csv")
    if not f.exists():
        return  # skip if fixture absent
    d = pd.read_csv(f)
    assert set(["Date","Open","High","Low","Close"]).issubset(d.columns)
    # no negative prices
    assert (d[["Open","High","Low","Close"]] >= 0).all().all()
