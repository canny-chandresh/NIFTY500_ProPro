import pandas as pd
from src.eligibility import apply_gates

def test_apply_gates_minimal():
    df = pd.DataFrame([{"Symbol":"TEST","Entry":100,"Target":105,"SL":97}])
    out = apply_gates(df, min_liq_value=0)
    assert len(out) == 1
