
import os, pandas as pd, datetime as dt
from .events import economic_event_guardrail
from .telegram import send_message
from .model_selector import choose_and_predict, choose_and_predict_full
from .sector import apply_sector_cap
from .kill_switch import evaluate_and_update
from .model_robust import heavy_retrain
from .regime import apply_regime_adjustments
from .smartmoney import smart_money_today
from .config import CONFIG
from .utils_time import should_send_now_ist

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

def run_paper_session(top_k=5, min_event_impact=3):
    ks = evaluate_and_update()
    status = ks.get("status","ACTIVE")
    ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
    reg = apply_regime_adjustments()

    # FULL preds for shadow (stub)
    full_preds, which_full = choose_and_predict_full()
    if full_preds is None or full_preds.empty: 
        return pd.DataFrame()
    full_preds = _apply_sms(full_preds)

    # Sector-cap + top-k
    safe = full_preds.copy()
    top = apply_sector_cap(safe.sort_values("proba", ascending=False).copy(), top_k=top_k)

    # Telegram text (sent only at 15:15 IST)
    lines = [f"*Top {top_k} — NIFTY500 Pro Pro ({which_full.upper()} + SMS + LightGate)*  ({ts})",
             f"_Regime: {reg['regime']}_"]
    for i, r in enumerate(top.itertuples(),1):
        entry = getattr(r,"Entry",0.0); sl = getattr(r,"SL",0.0); tgt=getattr(r,"Target",0.0)
        prob = getattr(r,"proba",0.0); sms = getattr(r,"sms_score",0.5)
        lines.append(f"{i}. *{r.Symbol}*  Buy {entry:.2f}  SL {sl:.2f}  Tgt {tgt:.2f}  Prob {prob:.2f}  SMS {sms:.2f}")
    text = "\n".join(lines)
    os.makedirs("reports", exist_ok=True)
    open("reports/telegram_sample.txt","w").write(text)
    if should_send_now_ist():
        try: send_message(text)
        except Exception: pass
    return top
