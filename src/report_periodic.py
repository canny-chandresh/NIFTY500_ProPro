# src/report_periodic.py
from __future__ import annotations
import os, datetime as dt
import pandas as pd

REPORTS_DIR = "reports"
DL = "datalake"

def _safe_read(path: str) -> pd.DataFrame:
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()

def build_periodic() -> str:
    """
    Build simple rolling aggregates for daily/weekly/monthly paper logs.
    Writes CSVs into reports/ and returns a short status string.
    Safe to run even if logs are missing (creates tiny placeholders).
    """
    os.makedirs(REPORTS_DIR, exist_ok=True)
    eq = _safe_read(os.path.join(DL, "paper_trades.csv"))
    op = _safe_read(os.path.join(DL, "options_paper.csv"))
    fu = _safe_read(os.path.join(DL, "futures_paper.csv"))

    def _add_day(df: pd.DataFrame, col: str = "Timestamp") -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()
        out = df.copy()
        if col in out.columns:
            out["Date"] = pd.to_datetime(out[col], errors="coerce").dt.date
        else:
            out["Date"] = dt.date.today()
        return out

    eqd, opd, fud = _add_day(eq), _add_day(op), _add_day(fu)

    # simple counts per day
    if not eqd.empty:
        eqd.groupby("Date").size().rename("equity_trades")\
            .to_csv(os.path.join(REPORTS_DIR, "agg_equity_daily.csv"))
    if not opd.empty:
        opd.groupby("Date").size().rename("options_trades")\
            .to_csv(os.path.join(REPORTS_DIR, "agg_options_daily.csv"))
    if not fud.empty:
        fud.groupby("Date").size().rename("futures_trades")\
            .to_csv(os.path.join(REPORTS_DIR, "agg_futures_daily.csv"))

    # create a small index file so artifacts always contain something
    with open(os.path.join(REPORTS_DIR, "periodic_index.txt"), "w") as f:
        f.write(f"Periodic built at {dt.datetime.utcnow().isoformat()}Z\n")
        f.write(f"Equity rows: {0 if eq is None else len(eq)}\n")
        f.write(f"Options rows: {0 if op is None else len(op)}\n")
        f.write(f"Futures rows: {0 if fu is None else len(fu)}\n")

    return "periodic_ok"
