# src/corp_actions.py
from __future__ import annotations
import os, json, datetime as dt
import pandas as pd
from pathlib import Path
from config import CONFIG

DL = Path("datalake")
RAW = DL / "raw"
SYM = DL / "symbols"
CA_DIR = DL / "corp_actions"
ADJ = DL / "adjusted"
for p in [RAW, SYM, CA_DIR, ADJ]: p.mkdir(parents=True, exist_ok=True)

def load_index_membership() -> pd.DataFrame:
    f = SYM / "nifty500_members.csv"
    if f.exists():
        d = pd.read_csv(f)
        d["Symbol"] = d["Symbol"].astype(str).str.upper()
        return d
    return pd.DataFrame(columns=["Symbol","Sector"])

def ingest_bhavcopy_if_any() -> int:
    """
    If user has placed bhavcopy CSVs into datalake/raw/bhavcopy_YYYYMMDD.csv,
    collate latest into datalake/bhavcopy_latest.parquet
    Columns expected: SYMBOL, SERIES, OPEN, HIGH, LOW, CLOSE, TOTTRDQTY, TOTTRDVAL
    """
    raw_files = sorted((RAW).glob("bhavcopy_*.csv"))
    if not raw_files: return 0
    dfs = []
    for f in raw_files[-60:]:
        try:
            d = pd.read_csv(f)
            keep = ["SYMBOL","OPEN","HIGH","LOW","CLOSE","TOTTRDQTY","TOTTRDVAL"]
            d = d[[c for c in keep if c in d.columns]].copy()
            d["when"] = f.stem.split("_")[-1]
            dfs.append(d)
        except Exception:
            continue
    if not dfs: return 0
    out = pd.concat(dfs, ignore_index=True)
    out.rename(columns={
        "SYMBOL":"Symbol","OPEN":"Open","HIGH":"High","LOW":"Low","CLOSE":"Close",
        "TOTTRDQTY":"Volume","TOTTRDVAL":"Notional"
    }, inplace=True)
    out["Symbol"] = out["Symbol"].astype(str).str.upper()
    out.to_parquet(DL / "bhavcopy_latest.parquet", index=False)
    return len(out)

def ingest_corporate_actions() -> pd.DataFrame:
    """
    Expect optional file: datalake/corp_actions/corp_actions.csv
    Columns: Symbol, action, ratio, ex_date, cash_dividend
    action in {split, bonus, dividend}
    ratio like '2:1' meaning 2 new for 1 old (bonus), or '1:2' split (new:old)
    """
    f = CA_DIR / "corp_actions.csv"
    if f.exists():
        d = pd.read_csv(f)
        d["Symbol"] = d["Symbol"].astype(str).str.upper()
        d["ex_date"] = pd.to_datetime(d["ex_date"], errors="coerce")
        return d.sort_values("ex_date")
    return pd.DataFrame(columns=["Symbol","action","ratio","ex_date","cash_dividend"])

def _ratio_to_float(r: str) -> float:
    try:
        a,b = r.split(":"); a=float(a); b=float(b)
        if b == 0: return 1.0
        return a/b
    except Exception:
        return 1.0

def apply_corp_actions_to_ohlcv(ohlcv: pd.DataFrame, actions: pd.DataFrame) -> pd.DataFrame:
    """
    Applies split/bonus to price/volume; dividends optionally to total return series (adds 'AdjCloseTR').
    ohlcv must have: Date, Open, High, Low, Close, Volume
    """
    if ohlcv is None or ohlcv.empty: return ohlcv
    d = ohlcv.copy()
    d["Date"] = pd.to_datetime(d["Date"], errors="coerce")
    d = d.sort_values("Date")
    mult = pd.Series(1.0, index=d.index)

    for _, row in actions.iterrows():
        ex = row.get("ex_date")
        if pd.isna(ex): continue
        typ = str(row.get("action","")).lower()
        ratio = _ratio_to_float(str(row.get("ratio","1:1")))
        if typ in ("split","bonus"):
            # after ex-date, price scales by 1/ratio; volume scales by ratio
            mask = d["Date"] >= ex
            mult.loc[mask] = mult.loc[mask] / ratio

    # apply multiplicative factor
    for c in ("Open","High","Low","Close"):
        if c in d.columns: d[c] = d[c] * mult.values
    if "Volume" in d.columns: d["Volume"] = d["Volume"] * (1.0/mult.values)

    # total-return adjustment (dividends)
    if CONFIG.get("corp_actions",{}).get("dividends_to_total_return", True):
        d["AdjCloseTR"] = d["Close"].astype(float)
        divs = actions[actions["action"].str.lower()=="dividend"].copy() if "action" in actions.columns else pd.DataFrame()
        if not divs.empty and "cash_dividend" in divs.columns:
            for _, r in divs.iterrows():
                ex = r.get("ex_date"); amt = float(r.get("cash_dividend",0.0) or 0.0)
                if pd.isna(ex) or amt==0.0: continue
                d.loc[d["Date"]>=ex, "AdjCloseTR"] = d.loc[d["Date"]>=ex, "AdjCloseTR"] + amt
    return d

def adjust_all_per_symbol():
    """
    For each datalake/per_symbol/<SYMBOL>.csv, apply actions and write datalake/adjusted/<SYMBOL>.csv
    """
    per = DL / "per_symbol"
    if not per.exists(): return 0
    actions = ingest_corporate_actions()
    count = 0
    for f in per.glob("*.csv"):
        try:
            sym = f.stem.upper()
            ohlcv = pd.read_csv(f)
            act = actions[actions["Symbol"]==sym]
            adj = apply_corp_actions_to_ohlcv(ohlcv, act)
            out = ADJ / f.name
            adj.to_csv(out, index=False)
            count += 1
        except Exception:
            continue
    # marker
    json.dump({"when": dt.datetime.utcnow().isoformat()+"Z", "adjusted": count}, open(ADJ/"_meta.json","w"), indent=2)
    return count
