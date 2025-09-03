# src/pipeline.py
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

def _apply_sms(preds):
    if preds is None or preds.empty: return preds
    sms = smart_money_today(preds["Symbol"].tolist())
    if sms.empty:
        preds["sms_score"] = 0.5
        preds["sms_reasons"] = "neutral"
        return preds
    x = preds.merge(sms, on="Symbol", how="left")
    x["sms_score"] = x["sms_score"].fillna(0.5)
    x["sms_reasons"] = x["sms_reasons"].fillna("neutral")
    boost = float(CONFIG["smart_money"]["proba_boost"])
    x["proba"] = x["proba"] * (1.0 + boost * (x["sms_score"] - 0.5))
    min_sms = float(CONFIG["smart_money"]["min_sms"])
    x = x[x["sms_score"] >= min_sms].copy()
    return x

def _enrich_reasons(top):
    if not CONFIG["features"]["sr_pivots_v1"] or top is None or top.empty:
        return top
    top = top.copy()
    if "Reason" not in top.columns:
        top["Reason"] = ""
    top["Reason"] = top["Reason"].fillna("") + " | Rules: EMA/PP/gap checks"
    return top

def run_paper_session(top_k=5):
    ks = evaluate_and_update()
    status = ks.get("status","ACTIVE")
    reg = apply_regime_adjustments()
    ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M")

    preds, which_full = choose_and_predict_full()
    if preds is None or preds.empty:
        return pd.DataFrame()
    preds = _apply_sms(preds)

    safe = preds.copy()
    top = apply_sector_cap(safe.sort_values("proba", ascending=False).copy(), top_k=top_k)
    top = _enrich_reasons(top)

    if CONFIG["features"]["killswitch_v1"] and status == "SUSPENDED":
        text = f"*Top {top_k} — SUSPENDED by Kill-Switch*  ({ts})\n_Reason: Win-rate below floor_\nRegime: {reg['regime']} ({reg['reason']})"
    else:
        lines = [f"*Top {top_k} — NIFTY500 Pro Pro ({which_full.upper()} + SMS)*  ({ts})",
                 f"_Regime: {reg['regime']} ({reg['reason']})_"]
        for i, r in enumerate(top.itertuples(),1):
            entry = getattr(r,"Entry",0.0); sl = getattr(r,"SL",0.0); tgt=getattr(r,"Target",0.0)
            prob = getattr(r,"proba",0.0); sms = getattr(r,"sms_score",0.5)
            rsn  = getattr(r,"Reason","")
            lines.append(f"{i}. *{r.Symbol}*  Buy {entry:.2f}  SL {sl:.2f}  Tgt {tgt:.2f}  Prob {prob:.2f}  SMS {sms:.2f}\n    _{rsn}_")
        text = "\n".join(lines)

    os.makedirs("reports", exist_ok=True)
    open("reports/telegram_sample.txt","w").write(text)
    if should_send_now_ist():
        try: send_message(text)
        except Exception: pass
    return top
# --- append helpers (add near the bottom of src/pipeline.py) ---
import pandas as pd, os

def _append_csv(df, path):
    if df is None or df.empty:
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if os.path.exists(path):
        base = pd.read_csv(path)
        df = pd.concat([base, df], ignore_index=True)
    df.to_csv(path, index=False)

# Example usage inside run_paper_session(), AFTER 'top' is computed:
# from options_executor import simulate_from_equity_recos
# opt_df = simulate_from_equity_recos(top)
# _append_csv(opt_df, "datalake/options_paper.csv")

# (Do similarly for futures if you’ve added a futures simulator.)
