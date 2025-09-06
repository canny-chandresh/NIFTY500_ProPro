from src.greeks import black_scholes_greeks, implied_vol_newton

def test_greeks_basic():
    g = black_scholes_greeks(100, 100, 0.25, 0.06, 0.2, call=True)
    assert "delta" in g and "gamma" in g
    iv = implied_vol_newton(100, 100, 0.25, 0.06, 5.0, call=True)
    assert iv > 0
