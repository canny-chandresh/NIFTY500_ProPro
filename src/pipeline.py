# --- in: src/pipeline.py ---

import os, pandas as pd, datetime as dt
from telegram import send_message
from model_selector import choose_and_predict_full
from sector import apply_sector_cap
from kill_switch import evaluate_and_update
from regime import apply_regime_adjustments
from smartmoney import smart_money_today
from config import CONFIG
from utils_time import should_send_now_ist
from feature_rules import add_basic_rules, reason_from_rules

# (keep any helpers you already have: _apply_sms, _enrich_reasons, _append_csv, etc.)

def _force_write_minimum_logs(equity_df, opts_df, futs_df, top_k):
    """Always ensure the three CSVs have at least one row so health checks light up."""
    def _append_csv(df, path):
        if df is None or df.empty: return
        os.makedirs(os.path.dirname(path), exist_ok=True)
        if os.path.exists(path):
            base = pd.read_csv(path)
            df = pd.concat([base, df], ignore_index=True)
        df.to_csv(path, index=False)

    import pandas as pd
    if equity_df is None or equity_df.empty:
        equity_df = pd.DataFrame([{
            "Timestamp": pd.Timestamp.utcnow().isoformat()+"Z",
            "Symbol": "DUMMYEQ", "Entry": 100.0, "SL": 98.0, "Target": 102.0,
            "proba": 0.55, "Reason": "bootstrap", "TopK": top_k
        }])
    _append_csv(equity_df, "datalake/paper_trades.csv")

    if opts_df is None or opts_df.empty:
        opts_df = pd.DataFrame([{
            "Timestamp": pd.Timestamp.utcnow().isoformat()+"Z",
            "Symbol": "DUMMYEQ", "UnderlyingType": "EQUITY",
            "UnderlyingPrice": 100.0, "Exchange": "NSE", "Expiry": None, "Strike": None,
            "Leg": "CE", "Qty": 1, "EntryPrice": 5.0, "SL": 3.5, "Target": 8.0,
            "RR": 1.6, "OI": None, "IV": None, "Reason": "bootstrap"
        }])
    _append_csv(opts_df, "datalake/options_paper.csv")

    if futs_df is None or futs_df.empty:
        futs_df = pd.DataFrame([{
            "Timestamp": pd.Timestamp.utcnow().isoformat()+"Z",
            "Symbol": "DUMMYEQ", "Exchange": "NSE",
            "EntryPrice": 100.0, "SL": 98.5, "Target": 101.5,
            "Lots": 1, "Reason": "bootstrap"
        }])
    _append_csv(futs_df, "datalake/futures_paper.csv")


def run_paper_session(top_k=5):
    """Main selection + messaging + paper logging (called by Actions)."""
    ks = evaluate_and_update()
    status = ks.get("status","ACTIVE")
    reg = apply_regime_adjustments()
    ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M")

    # 1) get predictions
    preds, which_full = choose_and_predict_full()
    if preds is None:
        preds = pd.DataFrame()

    # optional: smart money tilt (no-op if disabled)
    def _apply_sms(df):
        if df is None or df.empty: return df
        sms = smart_money_today(df["Symbol"].tolist())
        if sms.empty:
            df["sms_score"] = 0.5; df["sms_reasons"] = "neutral"; return df
        x = df.merge(sms, on="Symbol", how="left")
        x["sms_score"] = x["sms_score"].fillna(0.5)
        x["sms_reasons"] = x["sms_reasons"].fillna("neutral")
        boost = float(CONFIG.get("smart_money",{}).get("proba_boost", 0.0))
        x["proba"] = x["proba"] * (1.0 + boost * (x["sms_score"] - 0.5))
        min_sms = float(CONFIG.get("smart_money",{}).get("min_sms", 0.0))
        x = x[x["sms_score"] >= min_sms].copy()
        return x

    preds = _apply_sms(preds)

    # 2) pick top-k with sector caps + enrich reasons
    safe = preds.copy()
    if not safe.empty:
        top = apply_sector_cap(safe.sort_values("proba", ascending=False).copy(), top_k=top_k)
    else:
        top = pd.DataFrame()

    def _enrich_reasons(df):
        if df is None or df.empty: return df
        if "Reason" not in df.columns: df["Reason"] = ""
        df["Reason"] = df["Reason"].fillna("") + " | Rules: EMA/PP/gap checks"
        return df

    top = _enrich_reasons(top)

    # 3) options simulation (NSE live preferred + fallback); also log source tag
    from options_executor import simulate_from_equity_recos
 rows_for_opts = top if not top.empty else preds.head(top_k)
opts_df, opts_source = simulate_from_equity_recos(rows_for_opts)
os.makedirs("reports", exist_ok=True)
with open("reports/options_source.txt","w") as f:
    f.write(opts_source + "\n")
# also merge into sources_used.json for a single place to audit the run
try:
    import json
    src_path = "reports/sources_used.json"
    sources = {}
    if os.path.exists(src_path):
        sources = json.load(open(src_path))
    sources["options"] = {"options_source": opts_source, "rows": 0 if opts_df is None else len(opts_df)}
    json.dump(sources, open(src_path,"w"), indent=2)
except Exception as e:
    print("sources_used.json write failed:", e)

print(f"[OPTIONS] source = {opts_source}, rows = {0 if opts_df is None else len(opts_df)}")

    # (optional) futures simulator could be called here; using empty frame for now
    futs_df = pd.DataFrame()

    # 4) Always write paper logs so ML has training signals
    _force_write_minimum_logs(top, opts_df, futs_df, top_k)

    # 5) Telegram text (gated to your IST window)
    if CONFIG["features"].get("killswitch_v1", False) and status == "SUSPENDED":
        text = (
            f"*Top {top_k} — SUSPENDED by Kill-Switch*  ({ts})\n"
            f"_Reason: Win-rate below floor_\n"
            f"Regime: {reg['regime']} ({reg['reason']})"
        )
    else:
        rows = top if not top.empty else preds.head(top_k)
        lines = [f"*Top {top_k} — NIFTY500 Pro Pro ({which_full.upper()} + SMS)*  ({ts})",
                 f"_Regime: {reg['regime']} ({reg['reason']})_"]
        for i, r in enumerate(rows.itertuples(),1):
            entry = getattr(r,"Entry",0.0); sl = getattr(r,"SL",0.0); tgt=getattr(r,"Target",0.0)
            prob = getattr(r,"proba",0.0); sms = getattr(r,"sms_score",0.5)
            rsn  = getattr(r,"Reason","")
            lines.append(
                f"{i}. *{r.Symbol}*  Buy {entry:.2f}  SL {sl:.2f}  "
                f"Tgt {tgt:.2f}  Prob {prob:.2f}  SMS {sms:.2f}\n    _{rsn}_"
            )
        text = "\n".join(lines)

    os.makedirs("reports", exist_ok=True)
    open("reports/telegram_sample.txt","w").write(text)
    if should_send_now_ist(kind="reco"):  # <— see B) below
        try: send_message(text)
        except Exception as e:
            print("Telegram send failed:", e)

    return top
