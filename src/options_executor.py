
import pandas as pd
from .config import CONFIG

def simulate_from_equity_recos(reco_df: pd.DataFrame)->pd.DataFrame:
    if reco_df is None or reco_df.empty:
        return pd.DataFrame(columns=["date","symbol","opt_style","ret_pct","pnl_pct","target_hit"])
    # Placeholder: zero PnL rows
    out = pd.DataFrame({
        "date": pd.Timestamp.today().date(),
        "symbol": reco_df["Symbol"],
        "opt_style": CONFIG["options"]["style"],
        "ret_pct": 0.0,
        "pnl_pct": 0.0,
        "target_hit": 0
    })
    return out
