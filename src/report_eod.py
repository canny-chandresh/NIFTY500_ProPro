# src/report_eod.py
from __future__ import annotations
import json, datetime as dt
from pathlib import Path
from typing import Dict, Any, List

import pandas as pd

# Optional imports
def _opt(name: str):
    try:
        return __import__(name)
    except Exception:
        return None

plt = None
try:
    import matplotlib.pyplot as _plt
    plt = _plt
except Exception:
    plt = None

from shap_explain import explain_tree_model
from sentiment import score_items

REP = Path("reports"); REP.mkdir(exist_ok=True)
EOD_TXT = REP / "eod_report.txt"
EOD_HTML = REP / "eod_report.html"
NEWS_LATEST = Path("datalake/news/news_latest.json")
FLOWS_LATEST = Path("datalake/flows/flows_latest.csv")
EXPL_DIR = Path("reports/explain")

def _read_trades() -> pd.DataFrame:
    # you may have paper_trades.csv under datalake
    p = Path("datalake/paper_trades.csv")
    if p.exists():
        try:
            return pd.read_csv(p, parse_dates=["timestamp"])
        except Exception:
            pass
    return pd.DataFrame(columns=["symbol","engine","pnl","timestamp","price","side"])

def _read_features_any() -> pd.DataFrame:
    # pick a representative matrix for SHAP demo
    ds = sorted(Path("datalake/features").glob("*_features.csv"))
    if not ds:
        return pd.DataFrame()
    try:
        df = pd.read_csv(ds[0])
        # Drop non-feature cols
        drop = {"Date","symbol","freq","asof_ts","regime_flag","y_1d",
                "live_source_equity","live_source_options","is_synth_options","data_age_min"}
        feat_cols = [c for c in df.columns if c not in drop and not c.endswith("_is_missing")]
        return df[feat_cols].dropna().tail(2000)
    except Exception:
        return pd.DataFrame()

def _read_news() -> List[Dict[str,Any]]:
    if NEWS_LATEST.exists():
        try:
            j = json.loads(NEWS_LATEST.read_text())
            return j.get("items", [])
        except Exception:
            return []
    return []

def _read_flows() -> pd.DataFrame:
    if FLOWS_LATEST.exists():
        try:
            return pd.read_csv(FLOWS_LATEST, parse_dates=["date"])
        except Exception:
            pass
    return pd.DataFrame(columns=["date","fii_net","dii_net","source"])

def _summarize_trades(trades: pd.DataFrame) -> Dict:
    if trades.empty:
        return {"count": 0}
    agg = trades.groupby("engine").agg(
        count=("pnl","size"),
        hit_rate=("pnl", lambda s: 100*float((s>0).mean())),
        pf=("pnl", lambda s: float(s[s>0].sum() / max(1e-9, -s[s<=0].sum())))
    ).reset_index()
    total = {
        "count": int(len(trades)),
        "hit_rate": float(100* (trades["pnl"]>0).mean()),
        "pf": float(trades[trades["pnl"]>0]["pnl"].sum() / max(1e-9, -trades[trades["pnl"]<=0]["pnl"].sum()))
    }
    return {"by_engine": agg, "total": total}

def build_eod() -> Dict:
    now = dt.datetime.utcnow().isoformat()+"Z"

    # 1) Trades summary
    trades = _read_trades()
    ts = _summarize_trades(trades)

    # 2) Explainability (best-effort)
    shap_png = None; shap_json = None; shap_top = {}
    X = _read_features_any()
    model = None
    # If you persist your ML model object, you can load here; otherwise this runs only if you inject one.
    # In practice, call explain_tree_model(model, X, tag="ml") from your training step and just read artifacts here.
    if not X.empty and model is not None:
        exp = explain_tree_model(model, X, topk_n=12, tag="ml")
        if exp.get("ok"):
            shap_png = exp.get("png")
            shap_json = exp.get("json")
            shap_top = exp.get("topk", {})

    # 3) News + sentiment
    news_items = _read_news()
    news_df = score_items(news_items) if news_items else pd.DataFrame()
    sent_mean = float(news_df["sentiment"].mean()) if not news_df.empty else 0.0
    top_news = []
    if not news_df.empty:
        # 3 best positive & 3 worst negative
        pos = news_df.sort_values("sentiment", ascending=False).head(3)
        neg = news_df.sort_values("sentiment", ascending=True).head(3)
        for _, r in pos.iterrows():
            top_news.append({"title": r["title"], "sentiment": float(r["sentiment"])})
        for _, r in neg.iterrows():
            top_news.append({"title": r["title"], "sentiment": float(r["sentiment"])})

    # 4) FII/DII flows
    flows = _read_flows()
    flows_tail = flows.tail(5).to_dict(orient="records") if not flows.empty else []

    # ---- TXT ----
    lines = []
    lines.append(f"EOD REPORT UTC: {now}")
    lines.append("-"*60)
    lines.append("TRADES:")
    if ts.get("count", 0) == 0 and not ts.get("by_engine", None):
        lines.append("  (no paper trades recorded)")
    else:
        t = ts["total"]
        lines.append(f"  Total: {t['count']} | HitRate: {t.get('hit_rate',0):.1f}% | PF: {t.get('pf',0):.2f}")
        if isinstance(ts["by_engine"], pd.DataFrame):
            for _, r in ts["by_engine"].iterrows():
                lines.append(f"  - {r['engine']}: n={int(r['count'])} | HR={r['hit_rate']:.1f}% | PF={r['pf']:.2f}")
    lines.append("")
    lines.append("EXPLAINABILITY:")
    if shap_png:
        lines.append(f"  SHAP image: {shap_png}")
    if shap_json:
        lines.append(f"  SHAP JSON : {shap_json}")
    if shap_top:
        lines.append("  Top features:")
        for k, v in shap_top.items():
            lines.append(f"    • {k}: {v:.4f}")
    else:
        lines.append("  (no SHAP available — model artifact not provided; safe to ignore)")
    lines.append("")
    lines.append("NEWS & SENTIMENT:")
    lines.append(f"  Mean sentiment: {sent_mean:+.3f}  (−1..+1)")
    for n in top_news:
        lines.append(f"    • {n['title'][:90]} …  ({n['sentiment']:+.2f})")
    if not top_news:
        lines.append("  (no news items)")
    lines.append("")
    lines.append("FII/DII FLOWS (last 5 rows):")
    if flows_tail:
        for r in flows_tail:
            lines.append(f"  {r.get('date','?')}: FII={r.get('fii_net','?')}  DII={r.get('dii_net','?')}  src={r.get('source','?')}")
    else:
        lines.append("  (no flows available)")

    EOD_TXT.write_text("\n".join(lines), encoding="utf-8")

    # ---- HTML (very minimal) ----
    html = ["<html><head><meta charset='utf-8'><title>EOD</title></head><body>"]
    html += [f"<h3>EOD REPORT UTC: {now}</h3>"]
    html += ["<h4>Trades</h4>"]
    if ts.get("count", 0) == 0 and not ts.get("by_engine", None):
        html += ["<p>(no paper trades recorded)</p>"]
    else:
        t = ts["total"]
        html += [f"<p>Total: {t['count']} | HitRate: {t.get('hit_rate',0):.1f}% | PF: {t.get('pf',0):.2f}</p>"]
        if isinstance(ts["by_engine"], pd.DataFrame):
            html += ["<ul>"]
            for _, r in ts["by_engine"].iterrows():
                html += [f"<li>{r['engine']}: n={int(r['count'])} | HR={r['hit_rate']:.1f}% | PF={r['pf']:.2f}</li>"]
            html += ["</ul>"]
    html += ["<h4>Explainability</h4>"]
    if shap_png:
        html += [f"<p><img src='../explain/{Path(shap_png).name}' width='560'></p>"]
    if shap_top:
        html += ["<ul>"] + [f"<li>{k}: {v:.4f}</li>" for k, v in shap_top.items()] + ["</ul>"]
    else:
        html += ["<p>(no SHAP available)</p>"]
    html += ["<h4>News & Sentiment</h4>"]
    html += [f"<p>Mean sentiment: {sent_mean:+.3f}</p>"]
    if top_news:
        html += ["<ul>"] + [f"<li>{n['title'][:120]} … ({n['sentiment']:+.2f})</li>" for n in top_news] + ["</ul>"]
    else:
        html += ["<p>(no news)</p>"]
    html += ["<h4>FII/DII Flows (tail)</h4>"]
    if flows_tail:
        html += ["<ul>"] + [f"<li>{r.get('date','?')}: FII={r.get('fii_net','?')}  DII={r.get('dii_net','?')}  src={r.get('source','?')}</li>" for r in flows_tail] + ["</ul>"]
    else:
        html += ["<p>(no flows)</p>"]
    html += ["</body></html>"]
    EOD_HTML.write_text("\n".join(html), encoding="utf-8")

    return {"ok": True, "txt": str(EOD_TXT), "html": str(EOD_HTML)}
