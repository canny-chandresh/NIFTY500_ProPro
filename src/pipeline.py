from __future__ import annotations
import os, json, datetime as dt
import pandas as pd

try:
    from config import CONFIG
except Exception:
    CONFIG = {}

def _try_import(name, default=None):
    try: return __import__(name, fromlist=["*"])
    except Exception: return default

_model_selector = _try_import("model_selector")
_sector        = _try_import("sector")
_kill_switch   = _try_import("kill_switch")
_regime        = _try_import("regime")
_smartmoney    = _try_import("smartmoney")
_utils_time    = _try_import("utils_time")
_telegram      = _try_import("telegram")
_report_eod    = _try_import("report_eod")
_report_period = _try_import("report_periodic")
_live_train    = _try_import("live_train")

def _ensure_reports(): os.makedirs("reports", exist_ok=True)
def _append_csv(df: pd.DataFrame, path: str):
    if df is None or df.empty: return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if os.path.exists(path):
        try:
            df = pd.concat([pd.read_csv(path), df], ignore_index=True)
        except Exception: pass
    df.to_csv(path, index=False)

def _make_dummy_equity(top_k=5):
    return pd.DataFrame([{
        "Timestamp": pd.Timestamp.utcnow().isoformat()+"Z",
        "Symbol":"DUMMYEQ","Entry":100.0,"SL":98.0,"Target":102.0,
        "proba":0.55,"Reason":"bootstrap","TopK": top_k
    }])

def _make_dummy_options():
    return pd.DataFrame([{
        "Timestamp": pd.Timestamp.utcnow().isoformat()+"Z",
        "Symbol":"DUMMYEQ","UnderlyingType":"EQUITY","UnderlyingPrice":100.0,
        "Exchange":"NSE","Expiry":None,"Strike":None,"Leg":"CE","Qty":1,
        "EntryPrice":5.0,"SL":3.5,"Target":8.0,"RR":1.6,"OI":None,"IV":None,
        "Reason":"bootstrap"
    }])

def _make_dummy_futures():
    return pd.DataFrame([{
        "Timestamp": pd.Timestamp.utcnow().isoformat()+"Z",
        "Symbol":"DUMMYEQ","UnderlyingType":"EQUITY","Exchange":"NSE","Expiry":None,
        "EntryPrice":100.0,"SL":98.5,"Target":101.5,"Lots":1,"Reason":"bootstrap"
    }])

def _force_write_minimum_logs(equity_df, opts_df, futs_df, top_k):
    if equity_df is None or equity_df.empty: equity_df = _make_dummy_equity(top_k)
    if opts_df   is None or opts_df.empty:   opts_df   = _make_dummy_options()
    if futs_df   is None or futs_df.empty:   futs_df   = _make_dummy_futures()
    _append_csv(equity_df, "datalake/paper_trades.csv")
    _append_csv(opts_df,   "datalake/options_paper.csv")
    _append_csv(futs_df,   "datalake/futures_paper.csv")

def _apply_sms(df):
    if df is None or df.empty or _smartmoney is None: return df
    try: sms = _smartmoney.smart_money_today(df["Symbol"].tolist())
    except Exception: return df
    if sms is None or sms.empty:
        df["sms_score"]=0.5; df["sms_reasons"]="neutral"; return df
    x = df.merge(sms, on="Symbol", how="left")
    x["sms_score"]=x["sms_score"].fillna(0.5); x["sms_reasons"]=x["sms_reasons"].fillna("neutral")
    boost = float(CONFIG.get("smart_money",{}).get("proba_boost",0.0))
    x["proba"] = x["proba"] * (1.0 + boost*(x["sms_score"]-0.5))
    min_sms = float(CONFIG.get("smart_money",{}).get("min_sms",0.0))
    return x[x["sms_score"]>=min_sms].copy()

def _enrich_reasons(df):
    if df is None or df.empty: return df
    if "Reason" not in df.columns: df["Reason"]=""
    if CONFIG.get("features",{}).get("sr_pivots_v1",False):
        df["Reason"] = df["Reason"].fillna("") + " | Rules: EMA/PP/gap checks"
    return df

def _should_send_now(kind="reco"):
    if _utils_time is None: return False
    try: return bool(_utils_time.should_send_now_ist(kind=kind))
    except Exception: return False

def _send_telegram(text: str):
    if _telegram is None: return
    try: _telegram.send_message(text)
    except Exception as e: print("Telegram send failed:", e)

def _sector_cap(df, top_k: int):
    if df is None or df.empty: return df
    if _sector is None or not CONFIG.get("selection",{}).get("sector_cap_enabled",False):
        return df.head(top_k)
    try: return _sector.apply_sector_cap(df, top_k=top_k)
    except Exception: return df.head(top_k)

def _kill_status():
    if _kill_switch is None or not CONFIG.get("features",{}).get("killswitch_v1",False): return "ACTIVE"
    try: return _kill_switch.evaluate_and_update().get("status","ACTIVE")
    except Exception: return "ACTIVE"

def _regime_tag():
    if _regime is None or not CONFIG.get("features",{}).get("regime_v1",False):
        return {"regime":"NA","reason":"regime_v1 disabled"}
    try: return _regime.apply_regime_adjustments()
    except Exception: return {"regime":"NA","reason":"unavailable"}

def _opts_from_equity(rows_for_opts: pd.DataFrame):
    tag, df = "synthetic", pd.DataFrame()
    try:
        from options_executor import simulate_from_equity_recos as _sim
        res = _sim(rows_for_opts)
        df, tag = (res if isinstance(res, tuple) else (res, "synthetic"))
    except Exception as e:
        print("options_executor failed:", e)
    _ensure_reports(); open("reports/options_source.txt","w").write(tag+"\n")
    print(f"[OPTIONS] source = {tag}, rows = {0 if df is None else len(df)}")
    return df, tag

def _futs_from_equity(rows_for_futs: pd.DataFrame):
    tag, df = "synthetic", pd.DataFrame()
    try:
        from futures_executor import simulate_from_equity_recos as _sim
        res = _sim(rows_for_futs)
        df, tag = (res if isinstance(res, tuple) else (res, "synthetic"))
    except Exception as e:
        print("futures_executor failed:", e)
    _ensure_reports(); open("reports/futures_source.txt","w").write(tag+"\n")
    print(f"[FUTURES] source = {tag}, rows = {0 if df is None else len(df)}")
    return df, tag

def _merge_sources_used(extra: dict):
    _ensure_reports()
    path = "reports/sources_used.json"; data = {}
    if os.path.exists(path):
        try: data = json.load(open(path))
        except Exception: data = {}
    data.update(extra or {})
    try: json.dump(data, open(path,"w"), indent=2)
    except Exception as e: print("sources_used.json write failed:", e)

def _source_footer(eq_info, opts_source, opts_df, futs_source, futs_df) -> str:
    eq_src  = (eq_info or {}).get("equities_source","unknown")
    eq_rows = (eq_info or {}).get("rows",0)
    op_rows = 0 if (opts_df is None or getattr(opts_df,"empty",True)) else len(opts_df)
    fu_rows = 0 if (futs_df is None or getattr(futs_df,"empty",True)) else len(futs_df)
    eq_badge = "LIVE" if str(eq_src).lower()=="yfinance" else f"SYN/{eq_src}"
    return (
        "\n— _Sources_: "
        f"Equities: {eq_badge} ({eq_rows}) • "
        f"Options: {opts_source} ({op_rows}) • "
        f"Futures: {futs_source} ({fu_rows})"
    )

def run_paper_session(top_k: int = 5) -> pd.DataFrame:
    ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
    status = _kill_status()
    regime = _regime_tag()

    eq_info = {}
    if _live_train is not None:
        try:
            eq_info = _live_train.ensure_equities_fresh(max_age_hours=6.0)
            print(f"[EQUITIES] source={eq_info.get('equities_source')} rows={eq_info.get('rows')} "
                  f"symbols={eq_info.get('symbols')} age_h={eq_info.get('age_hours')}")
            print(f"[TRAIN] {_live_train.train_all_modes_if_available()}")
        except Exception as e:
            print("live_train step failed:", e)

    preds, which_full = pd.DataFrame(), "light"
    try:
        if _model_selector and hasattr(_model_selector,"choose_and_predict_full"):
            preds, which_full = _model_selector.choose_and_predict_full()
    except Exception as e:
        print("model_selector.choose_and_predict_full error:", e); preds = pd.DataFrame()
    if preds is None: preds = pd.DataFrame()

    preds = _apply_sms(preds)
    top = pd.DataFrame()
    if not preds.empty:
        try: preds = preds.sort_values("proba", ascending=False).copy()
        except Exception: pass
        top = _sector_cap(preds, top_k=top_k)
        top = _enrich_reasons(top)

    rows_for_derivs = top if not top.empty else preds.head(top_k)
    opts_df, opts_source = _opts_from_equity(rows_for_derivs)
    futs_df, futs_source = _futs_from_equity(rows_for_derivs)
    _merge_sources_used({
        "options":{"options_source":opts_source,"rows":0 if opts_df is None else len(opts_df)},
        "futures":{"futures_source":futs_source,"rows":0 if futs_df is None else len(futs_df)},
    })

    _force_write_minimum_logs(top, opts_df, futs_df, top_k)
    footer = _source_footer(eq_info, opts_source, opts_df, futs_source, futs_df)

    if status=="SUSPENDED" and CONFIG.get("features",{}).get("killswitch_v1",False):
        text = (
            f"*Top {top_k} — SUSPENDED by Kill-Switch*  ({ts})\n"
            f"_Reason: Win-rate below floor_\n"
            f"Regime: {regime.get('regime')} ({regime.get('reason')})"
        ) + footer
    else:
        lines = [
            f"*Top {top_k} — NIFTY500 Pro Pro ({str(which_full).upper()} + SMS)*  ({ts})",
            f"_Regime: {regime.get('regime')} ({regime.get('reason')})_",
            "", "*Equity picks*"
        ]
        rows = top if not top.empty else preds.head(top_k)
        for i, r in enumerate(rows.itertuples(), 1):
            entry = getattr(r,"Entry",0.0); sl = getattr(r,"SL",0.0); tgt = getattr(r,"Target",0.0)
            prob  = getattr(r,"proba",0.0); sms = getattr(r,"sms_score",0.5) if hasattr(r,"sms_score") else 0.5
            rsn   = getattr(r,"Reason","")
            lines.append(
                f"{i}. *{r.Symbol}*  Buy {entry:.2f}  SL {sl:.2f}  Tgt {tgt:.2f}  "
                f"Prob {prob:.2f}  SMS {sms:.2f}\n    _{rsn}_"
            )
        if opts_df is not None and not opts_df.empty:
            lines += ["", "*Options (paper)*"]
            for rr in opts_df.head(3).itertuples():
                lines.append(
                    f"- {getattr(rr,'Symbol','?')} {getattr(rr,'Leg','?')} "
                    f"{getattr(rr,'Strike','')} {getattr(rr,'Expiry','')}  "
                    f"Entry {getattr(rr,'EntryPrice',0):.2f}  RR {(getattr(rr,'RR',0) or 0):.2f}"
                )
        if futs_df is not None and not futs_df.empty:
            lines += ["", "*Futures (paper)*"]
            for rr in futs_df.head(3).itertuples():
                lines.append(
                    f"- {getattr(rr,'Symbol','?')} {getattr(rr,'Expiry','')}  "
                    f"Entry {getattr(rr,'EntryPrice',0):.2f}  SL {getattr(rr,'SL',0):.2f}  Tgt {getattr(rr,'Target',0):.2f}"
                )
        text = "\n".join(lines) + footer

    _ensure_reports(); open("reports/telegram_sample.txt","w").write(text)
    if _should_send_now(kind="reco"): _send_telegram(text)
    return top
