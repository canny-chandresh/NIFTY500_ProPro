# src/champion.py
from __future__ import annotations
import os, json, datetime as dt
from pathlib import Path
from typing import Dict, Tuple
import pandas as pd

REP = Path("reports/champion"); REP.mkdir(parents=True, exist_ok=True)
DECISION = REP / "decision.json"

def _score_trades(trades: pd.DataFrame) -> Dict:
    if trades is None or trades.empty:
        return {"count": 0, "hit_rate": 0.0, "profit_factor": 0.0, "avg_pnl": 0.0}
    # Expect columns: pnl (R or pct), status, engine, when_utc
    d = trades.copy()
    if "pnl" not in d.columns:
        # stub: if missing, treat Target/SL nominal
        if {"Entry","Target","SL"} <= set(d.columns):
            d["pnl"] = ((d["Target"] - d["Entry"]).abs() - (d["Entry"] - d["SL"]).abs()).fillna(0.0)
        else:
            d["pnl"] = 0.0
    wins = (d["pnl"] > 0).sum()
    losses = (d["pnl"] <= 0).sum()
    pf = (d.loc[d["pnl"] > 0, "pnl"].sum() / max(1e-9, -d.loc[d["pnl"] <= 0, "pnl"].sum()))
    return {
        "count": int(len(d)),
        "hit_rate": round(100.0 * wins / max(1, len(d)), 2),
        "profit_factor": round(float(pf), 2),
        "avg_pnl": round(float(d["pnl"].mean() if len(d) else 0.0), 4),
    }

def load_paper_trades(path="datalake/paper_trades.csv", days=30) -> pd.DataFrame:
    if not os.path.exists(path): return pd.DataFrame()
    df = pd.read_csv(path)
    if "when_utc" in df.columns:
        df["when_utc"] = pd.to_datetime(df["when_utc"], errors="coerce")
        cutoff = pd.Timestamp.utcnow() - pd.Timedelta(days=days)
        df = df[df["when_utc"] >= cutoff]
    return df

def evaluate_challenger() -> Dict:
    """
    Compare 'champion' (current model) vs 'challenger' (alt config or alt selector).
    Strategy here:
      - Champion = metadata from reports/registry/last model + trades observed
      - Challenger = rerun model_selector with alt flag and score (paper trades required)
    For now we compare on recent paper trades tagged by engine column.
    """
    trades = load_paper_trades()
    # As a first cut, use AUTO (champion pipeline) vs ALGO (challenger exploration)
    ch = _score_trades(trades[trades["engine"]=="AUTO"]) if "engine" in trades.columns else _score_trades(trades)
    cc = _score_trades(trades[trades["engine"]=="ALGO"]) if "engine" in trades.columns else {"count":0,"hit_rate":0,"profit_factor":0,"avg_pnl":0}

    decision = {
        "when_utc": dt.datetime.utcnow().isoformat()+"Z",
        "champion": {"engine": "AUTO", **ch},
        "challenger": {"engine": "ALGO", **cc},
        "policy": {"promote_if": {"hit_rate_delta_min": 2.0, "pf_delta_min": 0.1, "min_trades": 40}}
    }

    # decision rule
    promote = False
    if cc["count"] >= decision["policy"]["promote_if"]["min_trades"]:
        if (cc["hit_rate"] - ch["hit_rate"] >= decision["policy"]["promote_if"]["hit_rate_delta_min"] and
            cc["profit_factor"] - ch["profit_factor"] >= decision["policy"]["promote_if"]["pf_delta_min"]):
            promote = True

    decision["promote"] = promote
    DECISION.write_text(json.dumps(decision, indent=2), encoding="utf-8")
    return decision

if __name__ == "__main__":
    print(json.dumps(evaluate_challenger(), indent=2))
