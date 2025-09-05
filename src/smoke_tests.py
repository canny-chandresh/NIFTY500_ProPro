# src/smoke_tests.py
from __future__ import annotations
import json, datetime as dt, os, importlib
import pandas as pd

RESULT = {"when_utc": dt.datetime.utcnow().isoformat()+"Z", "checks": []}

def _ok(name, detail=""):
    RESULT["checks"].append({"name": name, "status": "PASS", "detail": detail})

def _fail(name, err):
    RESULT["checks"].append({"name": name, "status": "FAIL", "detail": str(err)})

def check_imports():
    mods = [
        "config","utils_time","indicators","livefeeds","news_pulse","regime",
        "model_selector","options_executor","futures_executor","pipeline"
    ]
    for m in mods:
        try:
            importlib.import_module(m)
            _ok(f"import:{m}")
        except Exception as e:
            _fail(f"import:{m}", e)

def check_datalake_min():
    try:
        exists = os.path.isdir("datalake")
        eq_csv = os.path.exists("datalake/daily_equity.csv")
        per_n  = 0
        if os.path.isdir("datalake/per_symbol"):
            per_n = len([f for f in os.listdir("datalake/per_symbol") if f.endswith(".csv")])
        _ok("datalake presence", f"exists={exists}, daily_equity.csv={eq_csv}, per_symbol={per_n}")
    except Exception as e:
        _fail("datalake presence", e)

def check_indicators_ema():
    try:
        from indicators import ema
        s = pd.Series([1,2,3,4,5], dtype=float)
        e = ema(s, span=3)
        assert len(e) == 5 and not e.isna().all()
        _ok("indicators.ema", f"last={round(float(e.iloc[-1]),4)}")
    except Exception as e:
        _fail("indicators.ema", e)

def check_regime_tags():
    try:
        from regime import apply_regime_adjustments
        r = apply_regime_adjustments()
        assert isinstance(r, dict) and "regime" in r and "reason" in r
        _ok("regime.apply_regime_adjustments", f"{r.get('regime')} | {r.get('reason')}")
    except Exception as e:
        _fail("regime.apply_regime_adjustments", e)

def check_selector_and_executors():
    try:
        import pandas as pd
        from model_selector import choose_and_predict_full
        from options_executor import simulate_from_equity_recos as opt_sim
        from futures_executor import simulate_from_equity_recos as fut_sim

        # Minimal stub if no datalake yet: fabricate two rows
        preds, tag = choose_and_predict_full(top_k=5)
        if preds is None or preds.empty:
            preds = pd.DataFrame([{
                "Symbol":"RELIANCE.NS","Entry":2500.0,"SL":2450.0,"Target":2550.0,"proba":0.55,"Reason":"stub"
            },{
                "Symbol":"TCS.NS","Entry":3600.0,"SL":3525.0,"Target":3670.0,"proba":0.53,"Reason":"stub"
            }])

        odf, osrc = opt_sim(preds)
        fdf, fsrc = fut_sim(preds)
        assert isinstance(odf, pd.DataFrame) and isinstance(fdf, pd.DataFrame)
        _ok("model_selector.choose_and_predict_full", f"rows={0 if preds is None else len(preds)} tag={tag}")
        _ok("options_executor.simulate_from_equity_recos", f"rows={len(odf)} src={osrc}")
        _ok("futures_executor.simulate_from_equity_recos", f"rows={len(fdf)} src={fsrc}")
    except Exception as e:
        _fail("selector/executors", e)

def run_smoke():
    check_imports()
    check_datalake_min()
    check_indicators_ema()
    check_regime_tags()
    check_selector_and_executors()
    print("=== SMOKE JSON ===")
    print(json.dumps(RESULT, indent=2))
    # never raise; this is a smoke report

if __name__ == "__main__":
    run_smoke()
