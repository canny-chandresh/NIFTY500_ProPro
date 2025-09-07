# src/features_builder.py
# Fully-patched feature matrix builder:
# - Universal keys: symbol/freq/asof_ts/data_age_min
# - Source flags: live_source_equity, live_source_options, is_synth_options
# - Regime/context compatibility
# - Graph features merge (weekly)
# - AUTO feature promotions support
# - Feature namespaces + _is_missing masks
# - Meta sidecar JSON per build
# - build_all() helper

from __future__ import annotations
from pathlib import Path
from typing import Dict, List, Optional
import datetime as dt
import json

import numpy as np
import pandas as pd

try:
    import yaml  # optional; used for manual feature spec and promotions
except Exception:
    yaml = None

# ----------------- Paths -----------------
DL = Path("datalake")
PER = DL / "per_symbol"           # raw per-symbol CSVs (Date, Open, High, Low, Close, Volume)
OUT_DIR = DL / "features"         # per-symbol feature matrices
AUTO_DIR = DL / "features_auto"   # auto-discovered candidates per symbol (Date + columns)
META_DIR = DL / "features_meta"   # sidecar JSON per build

SPEC = Path("config/feature_spec.yaml")         # manual features
PROM = Path("config/promoted_features.yaml")    # auto-promoted features catalog (AUTO_*)

OUT_DIR.mkdir(parents=True, exist_ok=True)
META_DIR.mkdir(parents=True, exist_ok=True)

# ----------------- Basic primitives -----------------
def pct_change(series: pd.Series, n: int) -> pd.Series:
    return series.pct_change(n)

def atr(high: pd.Series, low: pd.Series, close: pd.Series, n: int = 14) -> pd.Series:
    tr = np.maximum.reduce([
        (high - low).to_numpy(),
        (high - close.shift()).abs().to_numpy(),
        (low - close.shift()).abs().to_numpy()
    ])
    tr = pd.Series(tr, index=close.index)
    return tr.rolling(n).mean()

def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()

def slope(series: pd.Series, window: int = 10) -> pd.Series:
    return series.diff(window) / (window + 1e-9)

def rolling_beta(x: pd.Series, y: pd.Series, n: int = 60) -> pd.Series:
    cov = x.rolling(n).cov(y)
    var = y.rolling(n).var()
    return cov / (var + 1e-9)

def _winsorize(s: pd.Series, lo_q: float = 0.01, hi_q: float = 0.99) -> pd.Series:
    try:
        lo, hi = s.quantile(lo_q), s.quantile(hi_q)
        return s.clip(lo, hi)
    except Exception:
        return s

def _zscore(s: pd.Series, w: int) -> pd.Series:
    m = s.rolling(w).mean()
    v = s.rolling(w).std()
    return (s - m) / (v + 1e-9)

def _safe_read_csv(p: Path) -> pd.DataFrame:
    if not p.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(p, parse_dates=["Date"])
    except Exception:
        return pd.read_csv(p)

# ----------------- YAML helpers -----------------
def _load_yaml(path: Path, default: dict) -> dict:
    if yaml is None or not path.exists():
        return default
    try:
        return yaml.safe_load(path.read_text()) or default
    except Exception:
        return default

# ----------------- Feature evaluators -----------------
def _compute_feature(df: pd.DataFrame, feat: dict) -> pd.Series:
    """
    Minimal expression support; extend as needed.
    Supported:
      - pct_change(close, N)
      - atr(high, low, close, 14)  -> normalized by Close
      - ema(close, N) slope
      - gap (open - prev_close) / ATR
      - rolling_beta(ret_1, nifty_ret_1, 60)  (requires those cols precomputed)
      - news_sentiment_score (if present)
    """
    expr = (feat.get("expr") or "").lower().strip()

    if expr.startswith("pct_change"):
        # pct_change(close, N)
        try:
            n = int(expr.split(",")[-1].strip(" )"))
            return pct_change(df["Close"], n)
        except Exception:
            return pd.Series(index=df.index, dtype=float)

    if expr.startswith("atr(") or "atr(" in expr:
        # normalized ATR
        try:
            at = atr(df["High"], df["Low"], df["Close"], 14)
            return (at / df["Close"]).clip(lower=0)
        except Exception:
            return pd.Series(index=df.index, dtype=float)

    if expr.startswith("ema(") or "ema(" in expr:
        # ema(close, N) and then slope over 10
        try:
            # parse span argument; naive split
            # e.g., "ema(close, 20)"
            n = int(expr.split(",")[1].strip(" )"))
            em = ema(df["Close"], n)
            return slope(em, 10)
        except Exception:
            return pd.Series(index=df.index, dtype=float)

    if "gap" in expr:
        # (Open - prev Close) / ATR
        try:
            at = atr(df["High"], df["Low"], df["Close"], 14)
            prev = df["Close"].shift(1)
            return (df["Open"] - prev) / (at + 1e-9)
        except Exception:
            return pd.Series(index=df.index, dtype=float)

    if "rolling_beta" in expr:
        # requires ret_1 and nifty_ret_1 in df
        if "ret_1" in df.columns and "nifty_ret_1" in df.columns:
            try:
                return rolling_beta(df["ret_1"], df["nifty_ret_1"], 60)
            except Exception:
                pass
        return pd.Series(index=df.index, dtype=float)

    if "news_sentiment_score" in expr and "news_sentiment_score" in df.columns:
        return df["news_sentiment_score"]

    # Unknown expression → empty series
    return pd.Series(index=df.index, dtype=float)

# ----------------- Graph features merge -----------------
def _merge_graph_features(frame: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """
    Attaches static weekly graph factors for the symbol:
      - graph_deg, graph_btw
    If file is missing or symbol not found, fills NaN.
    """
    p = DL / "features" / "graph_features_weekly.csv"
    if not p.exists():
        frame["graph_deg"] = np.nan
        frame["graph_btw"] = np.nan
        return frame

    try:
        gf = pd.read_csv(p)
        if "symbol" not in gf.columns:
            frame["graph_deg"] = np.nan
            frame["graph_btw"] = np.nan
            return frame
        row = gf[gf["symbol"] == symbol]
        if row.empty:
            frame["graph_deg"] = np.nan
            frame["graph_btw"] = np.nan
        else:
            frame["graph_deg"] = float(row["graph_deg"].iloc[0])
            frame["graph_btw"] = float(row["graph_btw"].iloc[0])
    except Exception:
        frame["graph_deg"] = np.nan
        frame["graph_btw"] = np.nan

    return frame

# ----------------- Source/freshness annotations -----------------
def _annotate_sources(frame: pd.DataFrame) -> pd.DataFrame:
    """
    Writes:
      - live_source_equity: 'yfinance' (default)  [best-effort label]
      - live_source_options: 'nse' | 'synthetic' | 'none'
      - is_synth_options: 0/1
      - asof_ts: UTC timestamp when matrix built
      - data_age_min: (placeholder 0.0) — can be wired to last-tick if available
    """
    frame["live_source_equity"] = "yfinance"
    frame["live_source_options"] = "none"
    frame["is_synth_options"] = 0

    # Look for the latest options parquet to infer source label (best-effort)
    try:
        opt_files = sorted(DL.glob("options_chain_*.parquet"))
        if opt_files:
            last = pd.read_parquet(opt_files[-1], columns=["source", "fetched_utc"]).tail(1)
            if not last.empty:
                src = str(last["source"].iloc[0])
                frame["live_source_options"] = src
                frame["is_synth_options"] = 1 if src == "synthetic" else 0
    except Exception:
        pass

    now = pd.Timestamp.utcnow()
    frame["asof_ts"] = now
    frame["data_age_min"] = 0.0  # you can compute exact staleness if you store last tick per row

    return frame

# ----------------- Namespace + missingness masks -----------------
def _add_namespace_and_masks(frame: pd.DataFrame, ns: str = "MAN") -> pd.DataFrame:
    """
    - For every column (excluding keys/targets/masks/prefixed) add <col>_is_missing mask if absent.
    - For any unprefixed feature column, add MAN_ prefix (keeps AUTO_, GRAPH_, OPT_, MAN_, y_)
    """
    keep_cols = {"Date", "symbol", "freq", "asof_ts", "regime_flag",
                 "live_source_equity", "live_source_options", "is_synth_options",
                 "data_age_min"}

    # ensure masks
    for col in list(frame.columns):
        if col in keep_cols or col.endswith("_is_missing"):
            continue
        mcol = f"{col}_is_missing"
        if mcol not in frame.columns:
            frame[mcol] = frame[col].isna().astype(int)

    # add namespace for plain feature names
    rename_map = {}
    for col in list(frame.columns):
        if col in keep_cols or col.endswith("_is_missing"):
            continue
        if col.startswith(("AUTO_", "GRAPH_", "OPT_", "MAN_", "y_")):
            continue
        rename_map[col] = f"{ns}_{col}"

    if rename_map:
        frame = frame.rename(columns=rename_map)
        # carry masks to new names
        for old, new in rename_map.items():
            mo, mn = f"{old}_is_missing", f"{new}_is_missing"
            if mo in frame.columns and mn not in frame.columns:
                frame = frame.rename(columns={mo: mn})

    return frame

# ----------------- Manual spec loaders -----------------
def _load_manual_spec() -> dict:
    default = {"features": [], "targets": []}
    return _load_yaml(SPEC, default)

def _load_promoted_auto() -> list[dict]:
    default = {"auto_features": []}
    d = _load_yaml(PROM, default)
    return d.get("auto_features", [])

# ----------------- Main builders -----------------
def build_matrix(symbol: str, freq: str = "1d") -> Dict:
    """
    Build a single symbol's feature matrix CSV + meta JSON.
    Returns a summary dict.
    """
    raw_path = PER / f"{symbol}.csv"
    if not raw_path.exists():
        raise FileNotFoundError(f"Raw symbol file not found: {raw_path}")

    df = _safe_read_csv(raw_path)
    if df.empty:
        raise ValueError(f"No data in {raw_path}")

    df = df.sort_values("Date").reset_index(drop=True)

    # convenience returns for some expressions (ret_1, nifty_ret_1 if present)
    df["ret_1"] = df["Close"].pct_change(1)
    # If you have NIFTY close aligned per row, fill nifty_ret_1; else leave NaN
    if "nifty_close" in df.columns:
        df["nifty_ret_1"] = df["nifty_close"].pct_change(1)

    # ---------- prepare output frame ----------
    out = pd.DataFrame({"Date": df["Date"].values})

    # 1) Manual features from SPEC
    spec = _load_manual_spec()
    for feat in spec.get("features", []):
        name = feat.get("name") or "feat"
        s = _compute_feature(df, feat)

        # optional transforms
        if "clip" in feat and isinstance(feat["clip"], list) and len(feat["clip"]) == 2:
            s = s.clip(feat["clip"][0], feat["clip"][1])
        if "winsorize" in feat and isinstance(feat["winsorize"], list) and len(feat["winsorize"]) == 2:
            s = _winsorize(s, feat["winsorize"][0], feat["winsorize"][1])
        if "zscore_window" in feat:
            try:
                s = _zscore(s, int(feat["zscore_window"]))
            except Exception:
                pass

        out[name] = s
        # mask added later by _add_namespace_and_masks

    # 2) AUTO-promoted features (if present for this symbol)
    auto_prom = _load_promoted_auto()
    auto_file = AUTO_DIR / f"{symbol}_auto.csv"
    if auto_file.exists() and auto_prom:
        try:
            auto_df = pd.read_csv(auto_file, parse_dates=["Date"])
            out = out.merge(auto_df, on="Date", how="left")
            # rename promoted AUTO features to AUTO_* consistently
            for item in auto_prom:
                src = item.get("source", "")
                if src.startswith("AUTO::"):
                    col = src.split("AUTO::", 1)[1]
                    if col in out.columns:
                        out.rename(columns={col: f"AUTO_{col}"}, inplace=True)
        except Exception:
            # if bad, continue without auto features
            pass

    # 3) Targets (keep simple; builder should not leak future)
    # Primary: next-day return
    if "Close" in df.columns:
        out["y_1d"] = df["Close"].shift(-1) / df["Close"] - 1
        # You can add more horizons if needed:
        # out["y_5d"] = df["Close"].shift(-5) / df["Close"] - 1

    # 4) Universal keys & context
    out["symbol"] = symbol
    out["freq"] = freq

    # regime flag compatibility (if upstream module wrote it in raw)
    if "regime_flag" not in out.columns:
        out["regime_flag"] = 0  # will be replaced later if your regime module merges proper flags

    # 5) Graph features
    out = _merge_graph_features(out, symbol=symbol)

    # 6) Source/freshness flags
    out = _annotate_sources(out)

    # 7) Namespace + masks
    out = _add_namespace_and_masks(out, ns="MAN")

    # 8) Final row cleaning:
    # Keep rows where Date exists and target isn't NaN; DO NOT drop on feature NaNs (masks exist)
    if "y_1d" in out.columns:
        out = out[~out["y_1d"].isna()].reset_index(drop=True)
    else:
        out = out.dropna(subset=["Date"]).reset_index(drop=True)

    # 9) Save matrix CSV
    fpath = OUT_DIR / f"{symbol}_features.csv"
    out.to_csv(fpath, index=False)

    # 10) Meta sidecar JSON
    try:
        meta = {
            "symbol": symbol,
            "freq": freq,
            "when_utc": dt.datetime.utcnow().isoformat() + "Z",
            "rows": int(len(out)),
            "cols": [c for c in out.columns if c != "Date"],
            "sources": {
                "equity": "yfinance",  # best-effort label (upgrade if you track per-row)
                "options": (str(out["live_source_options"].iloc[-1]) if len(out) else "none"),
                "synthetic_used": bool(int(out["is_synth_options"].fillna(0).max() if len(out) else 0)),
            },
        }
        (META_DIR / f"{symbol}__{freq}.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
    except Exception:
        # meta is optional; don't fail build
        pass

    return {
        "ok": True,
        "symbol": symbol,
        "freq": freq,
        "rows": int(len(out)),
        "path": str(fpath)
    }

def build_all(limit: Optional[int] = None, freq: str = "1d") -> Dict:
    """
    Build matrices for a batch of symbols found in datalake/per_symbol/.
    """
    files = sorted(PER.glob("*.csv"))
    if limit:
        files = files[:int(limit)]
    built, failed = 0, 0
    errors: List[Dict] = []

    for p in files:
        sym = p.stem
        try:
            res = build_matrix(sym, freq=freq)
            if res.get("ok"):
                built += 1
            else:
                failed += 1
                errors.append({"symbol": sym, "reason": "unknown"})
        except Exception as e:
            failed += 1
            errors.append({"symbol": sym, "error": repr(e)})

    summary = {"built": built, "failed": failed, "errors": errors}
    return summary

if __name__ == "__main__":
    print(build_all(limit=None))
