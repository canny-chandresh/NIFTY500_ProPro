# src/feature_factory.py
from __future__ import annotations
import os, json, math, datetime as dt
from pathlib import Path
from typing import Dict, List, Tuple
import numpy as np
import pandas as pd
from scipy.stats import spearmanr
try:
    from sklearn.feature_selection import mutual_info_regression
except Exception:
    mutual_info_regression = None  # stays optional

DL = Path("datalake")
PER = DL / "per_symbol"
FEAT = DL / "features"
AUTO = DL / "features_auto"
REP = Path("reports/auto_features")
for p in (AUTO, REP): p.mkdir(parents=True, exist_ok=True)

# ------- helpers
def _safe_read_csv(p: Path) -> pd.DataFrame:
    if not p.exists(): return pd.DataFrame()
    try: return pd.read_csv(p, parse_dates=["Date"])
    except Exception: return pd.read_csv(p)

def _atr(h, l, c, n=14):
    tr = np.maximum.reduce([h-l, (h-c.shift()).abs(), (l-c.shift()).abs()])
    return tr.rolling(n).mean()

def _zscore(x, win=60):
    m = x.rolling(win).mean(); s = x.rolling(win).std()
    return (x - m) / (s + 1e-9)

def _r_slope(x, win=10):
    return x.diff(win) / (win + 1e-9)

def _rsi(close, n=14):
    ch = close.diff()
    up = ch.clip(lower=0).rolling(n).mean()
    dn = -ch.clip(upper=0).rolling(n).mean()
    rs = up / (dn + 1e-9)
    return 100 - (100/(1+rs))

def _ic(a: pd.Series, b: pd.Series) -> float:
    a, b = a.align(b, join="inner")
    if len(a) < 50: return np.nan
    rho, _ = spearmanr(a.values, b.values, nan_policy="omit")
    return float(rho) if np.isfinite(rho) else np.nan

def _psi(ref: pd.Series, cur: pd.Series, bins=10) -> float:
    """Population Stability Index (rough); larger => drift."""
    ref = ref.dropna(); cur = cur.dropna()
    if len(ref) < 100 or len(cur) < 100: return np.nan
    q = np.quantile(ref, np.linspace(0,1,bins+1))
    q[0]-=1e-9; q[-1]+=1e-9
    r_hist, _ = np.histogram(ref, bins=q); c_hist, _ = np.histogram(cur, bins=q)
    r = r_hist / max(1, r_hist.sum()); c = c_hist / max(1, c_hist.sum())
    eps=1e-9
    return float(np.sum((c - r) * np.log((c+eps)/(r+eps))))

# ------- candidate generators
WINDOWS = [5, 10, 20, 50]
def _gen_candidates(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    if not {"Date","Open","High","Low","Close","Volume"} <= set(d.columns):
        return pd.DataFrame()
    d = d.sort_values("Date").reset_index(drop=True)

    out = pd.DataFrame({"Date": d["Date"].values})
    close = d["Close"]; high=d["High"]; low=d["Low"]; vol=d["Volume"]

    # 1) returns & zscores
    for w in WINDOWS:
        out[f"ret_{w}"] = close.pct_change(w)
        out[f"volz_{w}"] = _zscore(vol.pct_change().fillna(0).rolling(w).std(), win=60)

    # 2) ATR normalized & RSI
    atr14 = _atr(high, low, close, 14)
    out["atrN"] = (atr14 / close).clip(0, 0.2)
    out["rsi14"] = _rsi(close, 14)

    # 3) EMA slopes & deviations
    for w in WINDOWS:
        ema = close.ewm(span=w).mean()
        out[f"ema{w}_slope"] = _r_slope(ema, win=10)
        out[f"ema{w}_devN"]  = (close - ema) / (atr14 + 1e-9)

    # 4) gaps
    prev_close = close.shift(1)
    out["gapN"] = (d["Open"] - prev_close) / (atr14 + 1e-9)

    # 5) interactions (limited, safe)
    out["mom_vol_10"] = out["ret_10"] * out["volz_10"]
    out["rsi_atr"]    = (out["rsi14"] - 50)/50 * out["atrN"]

    # (optional) external cols already merged upstream (e.g., news_sentiment_score)
    if "news_sentiment_score" in d.columns:
        out["news_mom_5"] = d["news_sentiment_score"] * out["ret_5"].fillna(0)

    return out

def _score_candidates(sym: str, feat: pd.DataFrame, target_col="y_1d") -> Dict:
    """Compute IC (and MI if available) vs target, plus stability & drift."""
    base = _safe_read_csv(FEAT / f"{sym}_features.csv")
    if base.empty or target_col not in base.columns:
        return {"symbol": sym, "reason":"no_base_or_target"}

    J = base[["Date", target_col]].merge(feat, on="Date", how="inner").dropna()
    if len(J) < 200: return {"symbol": sym, "reason":"too_small"}

    # regime split if present
    reg = base.get("regime_flag", pd.Series(index=base.index, data=np.nan))
    J = J.merge(base[["Date","regime_flag"]], on="Date", how="left")

    scores = {}
    for col in J.columns:
        if col in ("Date", target_col, "regime_flag"): continue
        ic_full = _ic(J[col], J[target_col])
        # simple stability: IC in halves
        m = len(J)//2
        ic_h1 = _ic(J[col].iloc[:m], J[target_col].iloc[:m])
        ic_h2 = _ic(J[col].iloc[m:],  J[target_col].iloc[m:])
        stab = 1.0 - float(abs((ic_h1 or 0) - (ic_h2 or 0)))  # closer => more stable
        # drift PSI: last 60d vs prior 60d
        psi = np.nan
        if len(J) > 150:
            psi = _psi(J[col].iloc[-60:], J[col].iloc[-120:-60])
        mi = None
        if mutual_info_regression is not None:
            try:
                X = J[[col]].values
                y = J[target_col].values
                mi = float(mutual_info_regression(X, y, discrete_features=False, random_state=0)[0])
            except Exception:
                mi = None
        scores[col] = {"ic": ic_full, "ic_h1": ic_h1, "ic_h2": ic_h2, "stability": stab, "psi": psi, "mi": mi}

    return {"symbol": sym, "N": int(len(J)), "scores": scores}

def build_and_score_symbol(symbol: str, save_files=True) -> Dict:
    raw = _safe_read_csv(PER / f"{symbol}.csv")
    if raw.empty: return {"symbol": symbol, "reason":"no_raw"}
    cand = _gen_candidates(raw)
    if cand.empty: return {"symbol": symbol, "reason":"no_candidates"}
    if save_files:
        cand.to_csv(AUTO / f"{symbol}_auto.csv", index=False)
    scored = _score_candidates(symbol, cand, target_col="y_1d")
    if save_files:
        (REP / f"{symbol}_scores.json").write_text(json.dumps(scored, indent=2), encoding="utf-8")
    return scored

def run_universe(limit: int | None = 50) -> Dict:
    files = list(PER.glob("*.csv"))
    if limit: files = files[:limit]
    out = []
    for p in files:
        sym = p.stem
        try:
            out.append(build_and_score_symbol(sym))
        except Exception as e:
            out.append({"symbol": sym, "error": repr(e)})

    # global catalog of top candidates (by IC & MI)
    catalog = []
    for r in out:
        if not isinstance(r, dict) or "scores" not in r: continue
        sym = r["symbol"]; sc = r["scores"]
        for k, v in sc.items():
            if v.get("ic") is None or np.isnan(v["ic"]): continue
            catalog.append({
                "symbol": sym, "feature": k,
                "ic": v["ic"],
                "stability": v.get("stability"),
                "psi": v.get("psi"),
                "mi": v.get("mi")
            })
    cat_df = pd.DataFrame(catalog).sort_values(["ic","mi"], ascending=[False, False])
    cat_path = REP / "catalog.json"
    cat_df.to_json(cat_path, orient="records", indent=2)
    summary = {
        "when_utc": dt.datetime.utcnow().isoformat()+"Z",
        "scored": len(out),
        "catalog_path": str(cat_path),
        "top_preview": cat_df.head(20).to_dict(orient="records")
    }
    (REP / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary

if __name__ == "__main__":
    print(json.dumps(run_universe(limit=None), indent=2))
