from __future__ import annotations
import os
import pandas as pd

try:
    from config import CONFIG
except Exception:
    CONFIG = {"selection":{"sector_cap_enabled":False,"max_per_sector":2,"max_total":5},
              "modes":{"auto_top_k":5}}

def _load_sector_map():
    try:
        p = os.path.join("datalake","sector_map.csv")
        if os.path.exists(p):
            df = pd.read_csv(p)
            if "Symbol" not in df.columns: df = df.rename(columns={df.columns[0]:"Symbol"})
            if "Sector" not in df.columns:
                for c in ["Industry","SectorName"]:
                    if c in df.columns: df = df.rename(columns={c:"Sector"}); break
            if "Sector" not in df.columns: df["Sector"]="UNKNOWN"
            df["Symbol"]=df["Symbol"].astype(str).str.upper()
            return df[["Symbol","Sector"]]
    except Exception: pass
    return pd.DataFrame(columns=["Symbol","Sector"])

def _apply_sector_cap(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty: return df
    sel = (CONFIG or {}).get("selection", {}) if isinstance(CONFIG, dict) else {}
    if not bool(sel.get("sector_cap_enabled", False)):
        return df.head(int(sel.get("max_total",5)))
    max_per_sector=int(sel.get("max_per_sector",2)); max_total=int(sel.get("max_total",5))
    sector_map=_load_sector_map()
    if sector_map.empty: return df.head(max_total)
    merged=df.merge(sector_map,on="Symbol",how="left")
    merged["Sector"]=merged["Sector"].fillna("UNKNOWN")
    out=[]; counts={}
    for _,r in merged.iterrows():
        s=r["Sector"]; c=counts.get(s,0)
        if c<max_per_sector: out.append(r); counts[s]=c+1
        if len(out)>=max_total: break
    if not out: return df.head(max_total)
    out=pd.DataFrame(out); 
    if "Sector" in out.columns: out=out.drop(columns=["Sector"])
    return out

def _normalize_cols(df: pd.DataFrame, reason_tag: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["Symbol","Entry","SL","Target","proba","Reason"])
    d=df.copy()
    rename={"Stop":"SL","StopLoss":"SL","stop":"SL","TakeProfit":"Target","TP":"Target","take_profit":"Target",
            "Probability":"proba","score":"proba","Score":"proba"}
    for k,v in rename.items():
        if k in d.columns and v not in d.columns: d=d.rename(columns={k:v})
    for req in ["Symbol","Entry","SL","Target","proba"]:
        if req not in d.columns:
            if req=="Symbol": d["Symbol"]=""
            elif req=="Entry": d["Entry"]=d["Close"] if "Close" in d.columns else 0.0
            elif req=="SL":    base=d.get("Entry", d.get("Close", 0.0)); d["SL"]=base*0.99
            elif req=="Target":base=d.get("Entry", d.get("Close", 0.0)); d["Target"]=base*1.01
            elif req=="proba": d["proba"]=0.5
    d["Reason"]=d.get("Reason", reason_tag); d["Reason"]=d["Reason"].fillna(reason_tag)
    d=d[["Symbol","Entry","SL","Target","proba","Reason"]]
    d["Symbol"]=d["Symbol"].astype(str).str.upper()
    d=d.dropna(subset=["Symbol"]).reset_index(drop=True)
    if "proba" in d.columns: d=d.sort_values("proba",ascending=False,kind="mergesort")
    return d

def _try_dl(top_k:int):
    try:
        import dl_runner
        df, tag = dl_runner.predict_topk_if_ready(top_k=top_k)
        if df is not None and not df.empty and tag=="dl_ready":
            df=_normalize_cols(df,"DL-GRU"); df=_apply_sector_cap(df)
            return df, "dl"
        return pd.DataFrame(), tag
    except Exception:
        return pd.DataFrame(), "dl_error"

def _try_robust(top_k:int):
    try:
        from model_robust import predict_today as robust_predict
        df = robust_predict(top_k=top_k)
        if df is None or df.empty: return pd.DataFrame(), "robust_empty"
        df=_normalize_cols(df,"ROBUST-ML"); df=_apply_sector_cap(df)
        return df.head(top_k), "robust"
    except Exception:
        return pd.DataFrame(), "robust_error"

def _try_light(top_k:int):
    try:
        from model_swing import predict_today
        df = predict_today(top_k=top_k)
        if df is None or df.empty: return pd.DataFrame(), "light_empty"
        df=_normalize_cols(df,"LIGHT-ML"); df=_apply_sector_cap(df)
        return df.head(top_k), "light"
    except Exception:
        return pd.DataFrame(), "light_error"

def choose_and_predict_full(top_k:int=5):
    """
    Pick among {'dl','robust','light'} via ai_ensemble, then apply:
      - ai_policy (context thresholds / sizing / abstention / suspension)
      - risk_manager (hard caps)
    Returns: (final_df, tag)  tag∈{'dl','robust','light','none'}
    """
    try:
        from ai_ensemble import update_weights_from_recent, choose_model
        update_weights_from_recent(window_days=10)
        which, _ = choose_model()
    except Exception:
        which = "dl"

    tried = []
    def order():
        if which == "dl": return [_try_dl, _try_robust, _try_light]
        if which == "robust": return [_try_robust, _try_dl, _try_light]
        return [_try_light, _try_robust, _try_dl]

    raw, tag = pd.DataFrame(), "none"
    for fn in order():
        df, t = fn(top_k)
        tried.append(t)
        if df is not None and not df.empty:
            raw, tag = df.copy(), df["Reason"].iloc[0].split("-")[0].lower()
            break

    if raw is None or raw.empty:
        return pd.DataFrame(columns=["Symbol","Entry","SL","Target","proba","Reason"]), "none"

    # Apply AI policy + risk
    try:
        from ai_policy import build_context, apply_policy
        from risk_manager import apply_guardrails
        ctx = build_context()
        raw = apply_policy(raw, ctx)
        raw = apply_guardrails(raw)
    except Exception:
        pass

    # record which model actually used
    try:
        import json, datetime as dt
        p = "reports/metrics/ai_ensemble_state.json"
        os.makedirs(os.path.dirname(p) or ".", exist_ok=True)
        st = json.load(open(p)) if os.path.exists(p) else {}
        hist = st.get("history", [])
        hist.append({"when_utc": dt.datetime.utcnow().isoformat()+"Z", "which": tag})
        st["history"] = hist[-200:]
        json.dump(st, open(p,"w"), indent=2)
    except Exception:
        pass

    return raw, tag
