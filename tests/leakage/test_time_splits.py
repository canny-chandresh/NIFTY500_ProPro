import os, pandas as pd
from pathlib import Path
import pytest

FEAT_DIR = Path("datalake/features")

@pytest.mark.skipif(not FEAT_DIR.exists(), reason="no features")
def test_time_monotonic():
    files = list(FEAT_DIR.glob("*_features.csv"))
    if not files: pytest.skip("no features")
    df = pd.read_csv(files[0], parse_dates=["Date"])
    assert df["Date"].is_monotonic_increasing, "dates should be sorted ascending"
