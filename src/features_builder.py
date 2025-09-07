from __future__ import annotations
import pandas as pd, numpy as np
import yaml, os, datetime as dt
from pathlib import Path

DL = Path("datalake")
SPEC = Path("config/feature_spec.yaml")
OUT_DIR = Path("datalake/features")
OUT_DIR.mkdir(parents=True, exist_ok=True)

def pct_change(series, n):
    return series.pct_change(n)

def atr(high, low, close, n=14):
    tr = np.maximum.reduce([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs()
    ])
    return tr.rolling(n).mean()

def slope(series, window=10):
    return (series.diff(window) / window)

def rolling_beta(x, y, n=60):
    cov = x.rolling(n).cov(y)
    var = y.rolling(n).var()
    return cov / var

def build_matrix(symbol: str, spec_path: Path = SPEC) -> pd.DataFrame:
    """Build feature + target matrix for one symbol from YAML spec."""
    f = DL / f"per_symbol/{symbol}.csv"
    if not f.exists():
        raise FileNotFoundError(f"missing {f}")

    df = pd.read_csv(f, parse_dates=["Date"])
    df = df.sort_values("Date").reset_index(drop=True)

    spec = yaml.safe_load(spec_path.read_text())
    features, targets = spec.get("features", []), spec.get("targets", [])

    # compute features
    for feat in features:
        name = feat["name"]
        expr = feat["expr"]
        try:
            if "pct_change" in expr:
                n = int(expr.split(",")[-1].strip(") "))
                df[name] = pct_change(df["Close"], n)
            elif "atr" in expr:
                df[name] = atr(df["High"], df["Low"], df["Close"], 14) / df["Close"]
            elif "ema" in expr:
                n = int(expr.split(",")[1].strip(") "))
                df[name] = df["Close"].ewm(span=n).mean()
                df[name+"_slope"] = slope(df[name], 10)
            elif "gap_open_atr" in expr:
                df[name] = (df["Open"] - df["Close"].shift()) / atr(df["High"], df["Low"], df["Close"])
            # extend with more as needed
        except Exception as e:
            df[name] = np.nan
            print(f"[features_builder] error {name}: {e}")

        # Winsorize / normalize
        if "clip" in feat:
            lo, hi = feat["clip"]
            df[name] = df[name].clip(lo, hi)
        if "zscore_window" in feat:
            roll = df[name].rolling(feat["zscore_window"])
            df[name] = (df[name] - roll.mean()) / (roll.std() + 1e-9)

        # missing flags
        miss_flag = name + "_is_missing"
        df[miss_flag] = df[name].isna().astype(int)

    # compute targets
    for targ in targets:
        name = targ["name"]
        horizon = targ["expr"].split("horizon=")[-1].split(",")[0].replace("m","").replace("d","").strip()
        horizon = 15 if horizon=="15" else 60 if horizon=="60" else 1
        # dummy target: next-day return
        df[name] = df["Close"].shift(-1) / df["Close"] - 1

    # drop NaN rows from lookaheads
    df = df.dropna().reset_index(drop=True)

    out = OUT_DIR / f"{symbol}_features.csv"
    df.to_csv(out, index=False)
    return df

def build_all():
    per_symbol = DL / "per_symbol"
    if not per_symbol.exists():
        print("No per_symbol folder.")
        return
    for csv in per_symbol.glob("*.csv"):
        sym = csv.stem
        try:
            df = build_matrix(sym)
            print(f"built {sym}, {len(df)} rows")
        except Exception as e:
            print(f"error {sym}: {e}")

if __name__ == "__main__":
    build_all()
