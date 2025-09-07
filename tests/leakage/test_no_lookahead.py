import os, pandas as pd
from pathlib import Path
import pytest

FEAT_DIR = Path("datalake/features")

@pytest.mark.skipif(not FEAT_DIR.exists(), reason="no features yet")
def test_no_lookahead_targets():
    files = list(FEAT_DIR.glob("*_features.csv"))
    if not files:
        pytest.skip("no per-symbol features available")
    df = pd.read_csv(files[0])
    # heuristic: targets (y_*) must not correlate too highly with same-row returns
    if {"Close"} <= set(df.columns):
        r_now = df["Close"].pct_change()
        tcols = [c for c in df.columns if c.startswith("y_")]
        for t in tcols:
            corr = pd.concat([r_now, df[t]], axis=1).corr().iloc[0,1]
            assert abs(corr) < 0.8, f"{t} correlates with contemporaneous returns (possible leakage)"
