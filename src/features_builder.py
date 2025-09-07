# src/features_builder.py
from __future__ import annotations
import pandas as pd, numpy as np, yaml
from pathlib import Path

DL = Path("datalake")
PER = DL / "per_symbol"
OUT_DIR = DL / "features"
AUTO_DIR = DL / "features_auto"
SPEC = Path("config/feature_spec.yaml")
PROM = Path("config/promoted_features.yaml")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ------- basic primitives (extend as needed)
def pct_change(series, n): return series.pct_change(n)
def atr(high, low, close, n=14):
    tr = np.maximum.reduce([high-low, (high-close.shift()).abs(), (low-close.shift()).abs()])
    return tr.rolling(n).mean()
def slope(series, window=10): return series.diff(window) / (window + 1e-9)
def rolling_beta(x, y, n=60): return x.rolling(n).cov(y) / (y.rolling(n).var()+1e-9)

def _load_yaml(p: Path, default: dict) -> dict:
    if not p.exists(): return default
    return yaml.safe_load(p.read_text())

def _winsorize(s: pd.Series, lo=-3.0, hi=3.0):  # zscore or pct bounds later
    ql, qh = s.quantile(0.01), s.quantile(0.99)
    return s.clip(ql, qh)

def _zscore(s: pd.Series, w: int): 
    m = s.rolling(w).mean(); v = s.rolling(w).std()
    return (s - m) / (v + 1e-9)

def _compute_feature(df: pd.DataFrame, feat: dict) -> pd.Series:
    expr = feat["expr"].lower()
    if expr.startswith("pct_change"):
        n = int(expr.split(",")[-1].strip(" )"))
        return pct_change(df["Close"], n)
    if expr.startswith("atr(") or "atr(" in expr:
        return atr(df["High"], df["Low"], df["Close"], 14) / df["Close"]
    if expr.startswith("ema("):
        n = int(expr.split(",")[1].strip(" )"))
        ema = df["Close"].ewm(span=n).mean()
        return slope(ema, 10)
    if "gap" in expr:
        prev = df["Close"].shift(1)
        return (df["Open"] - prev) / (atr(df["High"], df["Low"], df["Close"],14) + 1e-9)
    if "rolling_beta" in expr:
        # stub; requires index return column; skip if absent
        if "nifty_ret_1" not in df.columns or "ret_1" not in df.columns:
            return pd.Series(index=df.index, dtype=float)
        return rolling_beta(df["ret_1"], df["nifty_ret_1"], 60)
    if "news_sentiment_score" in expr and "news_sentiment_score" in df.columns:
        return df["news_sentiment_score"]
    return pd.Series(index=df.index, dtype=float)

def build_matrix(symbol: str) -> pd.DataFrame:
    f = PER / f"{symbol}.csv"
    if not f.exists(): raise FileNotFoundError(f)
    df = pd.read_csv(f, parse_dates=["Date"]).sort_values("Date").reset_index(drop=True)

    spec = _load_yaml(SPEC, {"features": [], "targets": []})
    feats = spec.get("features", [])
    out = pd.DataFrame({"Date": df["Date"].values})

    # manual features
    for feat in feats:
        name = feat["name"]; s = _compute_feature(df, feat)
        if "clip" in feat and isinstance(feat["clip"], list) and len(feat["clip"])==2:
            s = s.clip(feat["clip"][0], feat["clip"][1])
        if "zscore_window" in feat:
            s = _zscore(s, int(feat["zscore_window"]))
        out[name] = s
        out[name + "_is_missing"] = out[name].isna().astype(int)

    # promoted AUTO features (per-symbol)
    prom = _load_yaml(PROM, {"auto_features": []}).get("auto_features", [])
    auto_p = AUTO_DIR / f"{symbol}_auto.csv"
    if auto_p.exists() and prom:
        auto_df = pd.read_csv(auto_p, parse_dates=["Date"])
        for item in prom:
            src = item.get("source","")
            if not src.startswith("AUTO::"): continue
            col = src.split("AUTO::",1)[1]
            if col in auto_df.columns:
                out[item["name"]] = auto_df[col]
                out[item["name"] + "_is_missing"] = out[item["name"]].isna().astype(int)

    # targets: basic safe placeholder (1d forward return)
    if "Close" in df.columns:
        out["y_1d"] = df["Close"].shift(-1) / df["Close"] - 1

    # final clean
    out = out.dropna().reset_index(drop=True)
    (OUT_DIR / f"{symbol}_features.csv").write_text(out.to_csv(index=False), encoding="utf-8")
    return out

def build_all(limit: int | None = None) -> dict:
    files = list(PER.glob("*.csv"))
    if limit: files = files[:limit]
    ok, fail = 0, 0
    for p in files:
        try:
            build_matrix(p.stem); ok += 1
        except Exception:
            fail += 1
    return {"built": ok, "failed": fail}

if __name__ == "__main__":
    print(build_all())
