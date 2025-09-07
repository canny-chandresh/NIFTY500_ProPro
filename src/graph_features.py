# src/graph_features.py
from __future__ import annotations
from pathlib import Path
import pandas as pd, numpy as np
import datetime as dt
try:
    import networkx as nx
except Exception:
    nx = None

DL = Path("datalake")
PER = DL / "per_symbol"
OUT = DL / "features"
OUT.mkdir(parents=True, exist_ok=True)

def build_weekly_graph_features(symbols: list[str] | None = None,
                                lookback_days: int = 60,
                                asof_utc: str | None = None) -> dict:
    """
    Build correlation graph up to 'asof_utc' (exclusive) -> no future leakage.
    Writes: datalake/features/graph_features_weekly.csv
    """
    if asof_utc is None:
        cutoff = pd.Timestamp.utcnow().normalize()  # start of today UTC
    else:
        cutoff = pd.Timestamp(asof_utc)

    files = list(PER.glob("*.csv"))
    if symbols: files = [PER/f"{s}.csv" for s in symbols if (PER/f"{s}.csv").exists()]

    rets = {}
    for p in files:
        try:
            df = pd.read_csv(p, parse_dates=["Date"])
            df = df[df["Date"] < cutoff].tail(lookback_days)
            if df.empty: continue
            rets[p.stem] = df["Close"].pct_change().rename(p.stem)
        except Exception:
            pass

    if len(rets) < 5 or nx is None:
        # write placeholder with build timestamp so hygiene can validate
        pd.DataFrame({"symbol":[],"graph_deg":[],"graph_btw":[],"built_utc":[pd.Timestamp.utcnow().isoformat()+"Z"]}) \
          .to_csv(OUT / "graph_features_weekly.csv", index=False)
        return {"ok": False, "reason": "insufficient_data_or_networkx_missing"}

    M = pd.concat(rets.values(), axis=1).dropna()
    C = M.corr().fillna(0)
    G = nx.from_pandas_adjacency(C)
    deg = nx.degree_centrality(G)
    btw = nx.betweenness_centrality(G)

    out = pd.DataFrame({
        "symbol": list(deg.keys()),
        "graph_deg": [deg[s] for s in deg],
        "graph_btw": [btw[s] for s in deg],
        "built_utc": pd.Timestamp.utcnow().isoformat()+"Z"
    })
    out.to_csv(OUT / "graph_features_weekly.csv", index=False)
    return {"ok": True, "symbols": int(len(out)), "asof": cutoff.isoformat()}
