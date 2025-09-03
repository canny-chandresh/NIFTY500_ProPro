# src/options_executor.py
import os, pandas as pd
from config import CONFIG, DL

def _load_ban_list():
    fp = DL("ban_list")
    if os.path.exists(fp):
        try:
            df = pd.read_csv(fp)
            col = [c for c in df.columns if c.lower().startswith("symbol")]
            if col: return set(df[col[0]].astype(str).str.upper())
        except Exception:
            pass
    return set()

def _passes_sanity(r)->bool:
    e = getattr(r,"Entry",0.0); sl = getattr(r,"SL",0.0); tg = getattr(r,"Target",0.0)
    rr = (tg - e) / max(1e-6, (e - sl)) if tg and e and sl else 0.0
    return rr >= float(CONFIG["options"]["min_rr"])

def simulate_from_equity_recos(reco_df: pd.DataFrame)->pd.DataFrame:
    if reco_df is None or reco_df.empty:
        return pd.DataFrame(columns=["date","symbol","opt_style","ret_pct","pnl_pct","target_hit"])
    ban = _load_ban_list()
    rows = []
    for r in reco_df.itertuples():
        sym = str(r.Symbol).upper()
        if sym in ban: continue
        if not _passes_sanity(r): continue
        rows.append({
            "date": pd.Timestamp.today().date(),
            "symbol": sym,
            "opt_style": CONFIG["options"]["style"],
            "ret_pct": 0.0,
            "pnl_pct": 0.0,
            "target_hit": 0
        })
    return pd.DataFrame(rows)
