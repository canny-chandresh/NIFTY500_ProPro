import pandas as pd
from src.symbols import normalize_symbol

def test_normalize():
    assert normalize_symbol("tcs-ltd") == "TCSLTD"
    assert normalize_symbol("HDFC Bank & Co") == "HDFC BANK AND CO"
