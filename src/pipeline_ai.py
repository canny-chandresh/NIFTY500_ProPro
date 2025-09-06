# src/pipeline_ai.py
from __future__ import annotations
import os, json, datetime as dt
import pandas as pd

from config import CONFIG
from error_logger import RunLogger
from config_guard import config_diff
from ai_policy import build_context
from model_selector import choose_and_predict_full
from risk_manager import apply_guardrails
from atr_tuner import update_from_metrics
from portfolio import optimize_weights
from validator import validate_orders_df
from news import fetch_and_update, build_news_features
from explain import run_explain_tree
from corp_actions import ingest_bhavcopy_if_any, adjust_all_per_symbol
from model_registry import register_model

# NEW hooks
from eligibility import apply_gates
from calendars import policy_window_block, macro_block
from sli import compute_sli, alert_if_bad

# Telegram (safe fallback)
try:
    from telegram import send_recommendations
except Exception:
    def send_recommendations(auto_df=None, algo_df=None, ai_df=None, header=None, parse_mode="Markdown"):
        print("[TELEGRAM Fallback]")
        if header: print(header)
        for name, df in [("AUTO", auto_df), ("ALGO", algo_df), ("AI", ai_df)]:
            print(f"{name}:\n{df if df is not None else '(none)'}")

DL = "datalake"; REP_DIR = "reports"; MET_DIR = os.path.join(REP_DIR, "metrics")

def _ensure_dirs():
    os.makedirs(DL, exist_ok=True); os.makedirs(REP_DIR, exist_ok=True); os.makedirs(MET_DIR, exist_ok=True)

def _utcnow(): return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def _log_json(path: str, data):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f: json.dump(data, f, indent=2)

def _append_csv(path: str, df: pd.DataFrame):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    if not os.path.exists(path): df.to_csv(path, index=False)
    else: df.to_csv(path, mode="a", index=False, header=False)

def _paper_fill_df(picks: pd.DataFrame, engine: str, default_mode: str = "swing") -> pd.DataFrame:
    if picks is None or picks.empty: return picks
    d = picks.copy()
    for col in ("Symbol","Entry","Target","SL","proba"):
        if col not in d.columns:
            if col=="Symbol": d["Symbol"]=""; elif col=="Entry": d["Entry"]=d.get("Close",0.0)
            elif col=="Target": d["Target"]=d.get("Entry",d.get("Close",0.0))*1.01
            elif col=="SL": d["SL"]=d.get("Entry",d.get("Close",0.0))*0.99
            elif col=="proba": d["proba"]=0.50
    if "size_pct" not in d.columns or d["size_pct"].isna().all(): n=max(1,len(d)); d["size_pct"]=round(1.0/n,4)
    if "mode" not in d.columns: d["mode"]=default_mode
    d["Symbol"]=d["Symbol"].astype(str).str.upper(); d["fill_price"]=pd.to_numeric(d["Entry"],errors="coerce")
    d["engine"]=engine.upper(); d["status"]="OPEN"; d["when_utc"]=_utcnow()
    keep=["when_utc","engine","mode","Symbol","Entry","fill_price","Target","SL","size_pct","proba","Reason","status"]
    keep=[k for k in keep if k in d.columns]; return d[keep]

def run_auto_and_algo_sessions(top_k_auto: int | None = None, top_k_algo: int | None = None) -> tuple[int,int]:
    _ensure_dirs(); logger = RunLogger(label="pipeline")
    auto_orders = pd.DataFrame(); algo_orders = pd.DataFrame(); model_tag="unknown"; ctx={}; cfg_diff={}

    with logger.capture_all("pipeline_run", swallow=True):

        # 0) Config diff snapshot
        with logger.section("config/diff"):
            from config import CONFIG as CFG
            cfg_diff = config_diff(CFG); logger.add_meta(config_hash=cfg_diff.get("cur_hash"))

        # 0a) SLI + alert
        with logger.section("data/sli"):
            try:
                sli = compute_sli()
                logger.add_meta(sli=sli)
                # best-effort Telegram alert
                try: alert_if_bad(sli, tg_send=lambda header: send_recommendations(header=header))
                except Exception: pass
            except Exception as e:
                print("[sli] error:", e)

        # 0b) Corporate actions normalization (if raw bhavcopy is present)
        if CONFIG.get("corp_actions",{}).get("apply_on_load", True):
            with logger.section("corp_actions/apply"):
                try:
                    n = ingest_bhavcopy_if_any()
                    c = adjust_all_per_symbol()
                    logger.add_meta(bhav_rows=n, adjusted_files=c)
                except Exception as e:
                    print("[corp_actions] error:", e)

        # 1) News fetch & update
        with logger.section("news/fetch"):
            try: news_info = fetch_and_update(); logger.add_meta(news_added=news_info.get("added",0))
            except Exception as e: print("[news] fetch error:", e)

        # 2) Context (VIX/regime/market state)
        with logger.section("context/build"):
            ctx = build_context(); logger.add_meta(context=ctx)

        # 3) Model selection & ranked picks
        with logger.section("model_select"):
            tk_auto = int(CONFIG.get("modes",{}).get("auto_top_k",5) if top_k_auto is None else top_k_auto)
            raw_df, model_tag = choose_and_predict_full(top_k=tk_auto)

        if raw_df is None or raw_df.empty:
            logger.add_meta(model_used=model_tag, auto_count=0, algo_count=0)
            log_path = logger.dump(); print(f"[pipeline] log written → {log_path}"); return 0,0

        # 3b) Merge news sentiment into features
        if CONFIG.get("features",{}).get("news_to_features", True):
            with logger.section("news/merge_features"):
                try:
                    sym_df = raw_df[["Symbol","Sector"]].drop_duplicates() if "Sector" in raw_df.columns else raw_df[["Symbol"]].assign(Sector="")
                    enriched = build_news_features(sym_df)
                    raw_df = raw_df.merge(enriched[["Symbol","news_sentiment_score"]], on="Symbol", how="left")
                    raw_df["news_sentiment_score"] = raw_df["news_sentiment_score"].fillna(0.0)
                except Exception as e:
                    print("[news] feature merge error:", e)

        # 4) Guardrails
        with logger.section("guardrails/AUTO"):
            auto_df = apply_guardrails(raw_df)

        # 4b) Eligibility + Calendars gating
        with logger.section("eligibility/calendars"):
            try:
                auto_df = apply_gates(auto_df, min_liq_value=float(CONFIG.get("market",{}).get("min_liquidity_value", 2_00_00_000)))
                # Policy windows (earnings/ex-dates) — block symbols near events
                today = pd.Timestamp.utcnow().tz_localize("UTC")
                mask = []
                for s in auto_df["Symbol"]:
                    mask.append(not policy_window_block(s, today, pre_days=1, post_days=0))
                auto_df = auto_df.loc[mask].reset_index(drop=True)
                # High-impact macro day → thin book
                if macro_block(today):
                    auto_df = auto_df.head(max(1, len(auto_df)//2))
            except Exception as e:
                print("[eligibility/calendars] error:", e)

        # 5) ALGO leftovers
        with logger.section("algo_split"):
            tk_algo = int(CONFIG.get("modes",{}).get("algo_top_k",10) if top_k_algo is None else top_k_algo)
            leftover = raw_df[~raw_df["Symbol"].isin(auto_df["Symbol"])].copy()
            algo_df = apply_guardrails(leftover.sort_values("proba", ascending=False).head(max(0, tk_algo))) if not leftover.empty else pd.DataFrame(columns=auto_df.columns)

        # 6) Portfolio sizing with sector caps & turnover
        with logger.section("portfolio/size"):
            last_w_path = os.path.join(DL, "last_weights.csv"); lastW = None
            if os.path.exists(last_w_path):
                try: lastW = pd.read_csv(last_w_path)
                except Exception: lastW = None
            try:
                auto_df = optimize_weights(
                    auto_df,
                    method=CONFIG.get("sizing",{}).get("auto_method","hrp"),
                    max_total_risk=float(CONFIG.get("sizing",{}).get("auto_total_risk",1.0)),
                    max_per_name=float(CONFIG.get("sizing",{}).get("auto_per_name_cap",0.25)),
                    sector_caps=CONFIG.get("sizing",{}).get("sector_caps",{}),
                    turnover_cap=CONFIG.get("sizing",{}).get("max_daily_turnover",None),
                    last_weights=lastW
                )
            except Exception as e: print("[portfolio] AUTO sizing fallback:", e)
            try:
                algo_df = optimize_weights(
                    algo_df,
                    method=CONFIG.get("sizing",{}).get("algo_method","equal"),
                    max_total_risk=float(CONFIG.get("sizing",{}).get("algo_total_risk",0.5)),
                    max_per_name=float(CONFIG.get("sizing",{}).get("algo_per_name_cap",0.20)),
                    sector_caps=CONFIG.get("sizing",{}).get("sector_caps",{}),
                    turnover_cap=None,
                    last_weights=None
                )
            except Exception as e: print("[portfolio] ALGO sizing fallback:", e)
            try:
                pd.concat([
                    auto_df[["Symbol","size_pct"]].assign(engine="AUTO"),
                    algo_df[["Symbol","size_pct"]].assign(engine="ALGO")
                ], ignore_index=True).to_csv(last_w_path, index=False)
            except Exception as e:
                print("[portfolio] persist last_weights error:", e)

        # 7) Orders & validate
        with logger.section("build_orders/persist"):
            auto_orders = _paper_fill_df(auto_df, engine="AUTO", default_mode="swing")
            algo_orders = _paper_fill_df(algo_df, engine="ALGO", default_mode="intraday")
            okA, probA = validate_orders_df(auto_orders) if not auto_orders.empty else (True, [])
            okB, probB = validate_orders_df(algo_orders) if not algo_orders.empty else (True, [])
            if not okA: print("[validator] AUTO issues:", probA)
            if not okB: print("[validator] ALGO issues:", probB)
            orders_path = os.path.join(DL, "paper_trades.csv")
            if not auto_orders.empty: _append_csv(orders_path, auto_orders)
            if not algo_orders.empty: _append_csv(orders_path, algo_orders)

        # 8) Explainability (stub — plug your real model to get true SHAP)
        if CONFIG.get("features",{}).get("explainability", True):
            with logger.section("explainability"):
                try:
                    feature_names = [c for c in auto_df.columns if c not in ("Symbol","Entry","Target","SL","Reason")]
                    class _Dummy:
                        def predict(self, X):
                            import numpy as np; return np.zeros(len(X))
                    model = _Dummy()
                    for s in auto_df["Symbol"].head(5):
                        Xs = auto_df[auto_df["Symbol"]==s][feature_names]
                        if not Xs.empty:
                            run_explain_tree(s, model, Xs, feature_names)
                except Exception as e:
                    print("[explain] error:", e)

        # 8b) Registry snapshot
        with logger.section("registry/save"):
            if CONFIG.get("registry",{}).get("enabled", True):
                register_model({"name":"master_selector", "params":{"context":ctx}, "metrics":{"sample":True}})

        # 8c) Deep Learning shadow training
with logger.section("dl/shadow_train"):
    try:
        from dl_models.master_dl import DeepLearningTrainer
        dl = DeepLearningTrainer(window_days=90)
        dl_metrics = dl.train()
        logger.add_meta(dl=dl_metrics)
    except Exception as e:
        print("[dl] error:", e)

        # 9) Telegram notify
        with logger.section("telegram/send"):
            header = f"NIFTY500 ProPro — {model_tag.upper()} — {dt.datetime.utcnow().strftime('%Y-%m-%d %H:%MZ')}"
            send_recommendations(auto_df=auto_orders, algo_df=algo_orders, ai_df=None, header=header)

        # 10) Snapshot metrics
        with logger.section("snapshot/metrics"):
            snap = {
                "when_utc": _utcnow(),
                "model_used": model_tag,
                "auto_count": int(len(auto_orders)),
                "algo_count": int(len(algo_orders)),
                "config_changed_keys": list(cfg_diff.get("changed", {}).keys())
            }
            _log_json(os.path.join(MET_DIR, "last_run.json"), snap)
            logger.add_meta(**snap)

        # 11) ATR tuner update
        with logger.section("atr_tuner/update"):
            update_from_metrics(ctx)

    log_path = logger.dump(); print(f"[pipeline] log written → {log_path}")
    return int(len(auto_orders)), int(len(algo_orders))

if __name__ == "__main__":
    a, b = run_auto_and_algo_sessions(); print(f"AUTO: {a}, ALGO: {b}")
