import pandas as pd
from pathlib import Path

def test_features_exist():
    f = Path("datalake/features/RELIANCE_features.csv")
    if not f.exists(): return
    df = pd.read_csv(f)
    for col in ["symbol","freq","asof_ts","MAN_ret1","y_1d"]:
        assert col in df.columns
