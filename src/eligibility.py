# src/eligibility.py
from __future__ import annotations
import os, re, csv, time, json
import datetime as dt
from pathlib import Path
from typing import Dict, List, Tuple, Set
import pandas as pd

DL = Path("datalake"); ELIG = DL / "eligibility"; ELIG.mkdir(parents=True, exist_ok=True)

BAN_CSV   = ELIG / "fo_ban.csv"      # columns: Symbol, asof
ASM_CSV   = ELIG / "asm_list.csv"    # columns: Symbol, asof, stage
GSM_CSV   = ELIG / "gsm_list.csv"    # columns: Symbol, asof, stage
LIQ_CSV   = ELIG / "liquidity.csv"   # columns: Symbol, adv_value, asof
LOTS_CSV  = ELIG / "lot_tick.csv"    # columns: Symbol, lot_size, tick_size, instrument (EQUITY/FUT/OPT)
ADV_FALLBACK = 2_00_00_000  # ₹2 Cr

HEADERS = {"User-Agent": "NIFTY500-ProPro/1.0"}

def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists(): return pd.DataFrame()
    try: return pd.read_csv(path)
    except Exception: return pd.DataFrame()

def load_ban_set() -> Set[str]:
    df = _read_csv(BAN_CSV)
    return set(df["Symbol"].astype(str).str.upper()) if not df.empty else set()

def load_asm_set() -> Set[str]:
    df = _read_csv(ASM_CSV)
    return set(df["Symbol"].astype(str).str.upper()) if not df.empty else set()

def load_gsm_set() -> Set[str]:
    df = _read_csv(GSM_CSV)
    return set(df["Symbol"].astype(str).str.upper()) if not df.empty else set()

def load_liquidity() -> pd.DataFrame:
    df = _read_csv(LIQ_CSV)
    if df.empty:
        return pd.DataFrame(columns=["Symbol","adv_value"]).assign(adv_value=ADV_FALLBACK)
    df["Symbol"] = df["Symbol"].astype(str).str.upper()
    df["adv_value"] = pd.to_numeric(df["adv_value"], errors="coerce").fillna(0.0)
    return df[["Symbol","adv_value"]]

def load_lot_tick() -> pd.DataFrame:
    df = _read_csv(LOTS_CSV)
    if df.empty:
        return pd.DataFrame(columns=["Symbol","lot_size","tick_size","instrument"])
    df["Symbol"] = df["Symbol"].astype(str).str.upper()
    df["lot_size"] = pd.to_numeric(df["lot_size"], errors="coerce").fillna(1).astype(int)
    df["tick_size"] = pd.to_numeric(df["tick_size"], errors="coerce").fillna(0.05)
    df["instrument"] = df.get("instrument","EQUITY").astype(str).str.upper()
    return df

def apply_gates(picks: pd.DataFrame, min_liq_value: float = ADV_FALLBACK) -> pd.DataFrame:
    """
    Removes symbols that violate ban/ASM/GSM/liquidity thresholds.
    Adds 'eligibility_reason' column with reasons (if any).
    """
    if picks is None or picks.empty: return picks
    d = picks.copy()
    d["Symbol"] = d["Symbol"].astype(str).str.upper()

    ban = load_ban_set(); asm = load_asm_set(); gsm = load_gsm_set(); liq = load_liquidity()
    d = d.merge(liq, on="Symbol", how="left")
    d["adv_value"] = d["adv_value"].fillna(0.0)

    reasons = []
    keep_mask = []
    for _, r in d.iterrows():
        sym = r["Symbol"]; rsn = []
        if sym in ban: rsn.append("FO_BAN")
        if sym in asm: rsn.append("ASM")
        if sym in gsm: rsn.append("GSM")
        if float(r["adv_value"]) < float(min_liq_value): rsn.append("LOW_LIQ")
        reasons.append(",".join(rsn))
        keep_mask.append(len(rsn) == 0)

    d["eligibility_reason"] = reasons
    return d.loc[keep_mask].reset_index(drop=True)
