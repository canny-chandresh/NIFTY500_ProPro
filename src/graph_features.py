# src/graph_features.py
from __future__ import annotations
from pathlib import Path
import pandas as pd, numpy as np
try:
    import networkx as nx
except Exception:
    nx = None

DL = Path("datalake")
PER = DL / "per_symbol"
OUT = DL / "features"
OUT.mkdir(parents=True, exist_ok=True)

def build_weekly_graph_features(symbols: list[str]|None=None, lookback=60) -> dict:
    """Builds a simple correlation graph & exports degree/centrality per symbol."""
    files = list(PER.glob("*.csv"))
    if symbols: files = [PER/f"{s}.csv" for s in symbols if (PER/f"{s}.csv").exists()]
    rets = {}
    for p in files:
        try:
            df = pd.read_csv(p, parse_dates=["Date"]).tail(lookback)
            rets[p.stem] = df["Close"].pct_change().rename(p.stem)
        except Exception:
            pass
    if len(rets) < 5 or nx is None:
        return {"ok": False, "reason": "not_enough_data_or_networkx_missing"}
    M = pd.concat(rets.values(), axis=1).dropna()
    C = M.corr().fillna(0)
    G = nx.from_pandas_adjacency(C)
    deg = nx.degree_centrality(G)
    btw = nx.betweenness_centrality(G)
    out = pd.DataFrame({"symbol": list(deg.keys()),
                        "graph_deg": [deg[s] for s in deg],
                        "graph_btw": [btw[s] for s in deg]})
    out.to_csv(OUT / "graph_features_weekly.csv", index=False)
    return {"ok": True, "symbols": len(out)}
