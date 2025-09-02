
import os, pandas as pd
from .config import CONFIG

def apply_sector_cap(df, top_k=5):
    if df is None or df.empty: return df
    if not CONFIG.get("selection",{}).get("sector_cap_enabled", True):
        return df.head(top_k)
    cap = int(CONFIG["selection"].get("sector_cap_k", 2))
    path = os.path.join("datalake", CONFIG["selection"].get("sector_map_csv","sector_map.csv"))
    mapping = None
    if os.path.exists(path):
        try:
            mapping = pd.read_csv(path)
        except Exception:
            mapping = None
    if mapping is None or not {"Symbol","Sector"}.issubset(mapping.columns):
        return df.head(top_k)
    x = df.merge(mapping[["Symbol","Sector"]], on="Symbol", how="left")
    picks, counts = [], {}
    for _, r in x.sort_values("proba", ascending=False).iterrows():
        sec = r.get("Sector","UNKNOWN")
        c = counts.get(sec,0)
        if c < cap:
            picks.append(r)
            counts[sec] = c+1
        if len(picks)>=top_k: break
    return pd.DataFrame(picks) if picks else df.head(top_k)
