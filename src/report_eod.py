from __future__ import annotations
import os, json, datetime as dt
import pandas as pd

TS = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
RDIR = "reports"

def _load_csv(p): 
    try:
        if os.path.exists(p): return pd.read_csv(p)
    except Exception: pass
    return pd.DataFrame()

def _load_json(p):
    try:
        if os.path.exists(p): return json.load(open(p))
    except Exception: pass
    return {}

def _section_txt(title, body): return f"\n===== {title} =====\n{body.strip()}\n"

def _fmt_rows(df, cols, n=20):
    if df is None or df.empty: return "(none)"
    try: return df[cols].head(n).to_string(index=False)
    except Exception: return df.head(n).to_string(index=False)

def _html_table(df, cols, caption, n=30):
    if df is None or df.empty: return f"<h3>{caption}</h3><p>(none)</p>"
    try: d = df[cols].head(n)
    except Exception: d = df.head(n)
    return f"<h3>{caption}</h3>" + d.to_html(index=False, escape=True)

def _read_text(path: str) -> str:
    try:
        if os.path.exists(path): return open(path).read().strip()
    except Exception: pass
    return "NA"

def _pnl_stats(df, entry="Entry", tgt="Target", sl="SL"):
    if df is None or df.empty: 
        return {"trades":0, "approx_winrate":"n/a", "approx_pnl":"n/a"}
    wins = (df[tgt] > df[entry]).sum()
    approx_winrate = wins / max(1,len(df))
    # crude pnl proxy: + (tgt-entry) for win, else (sl-entry)
    pnl = ((df[tgt]-df[entry]).clip(lower=0) + (df[sl]-df[entry]).clip(upper=0)).sum()
    return {"trades": int(len(df)), "approx_winrate": f"{approx_winrate*100:.0f}%", "approx_pnl": round(float(pnl),2)}

def build_eod():
    os.makedirs(RDIR, exist_ok=True)

    auto_eq = _load_csv("datalake/paper_trades.csv")
    algo_eq = _load_csv("datalake/algo_paper.csv")
    opts    = _load_csv("datalake/options_paper.csv")
    futs    = _load_csv("datalake/futures_paper.csv")

    shadow_wf    = _load_json("reports/shadow/walkforward_summary.json")
    shadow_drift = _load_json("reports/shadow/drift_snapshot.json")
    shadow_meta  = _load_json("reports/shadow/_shadow_run.json")
    dl_eval      = _load_json("reports/shadow/dl_eval.json")

    sources = _load_json("reports/sources_used.json")
    opts_src = _read_text("reports/options_source.txt")
    futs_src = _read_text("reports/futures_source.txt")

    auto_stats = _pnl_stats(auto_eq)
    algo_stats = _pnl_stats(algo_eq)

    # TEXT
    txt = []
    txt.append(f"NIFTY500 Pro Pro — EOD God Report  ({TS})\n")
    txt.append(_section_txt("SOURCES",
        f"Equity source   : {sources.get('daily',{}).get('equities_source','yfinance')} / {sources.get('hourly',{}).get('interval','60m')}\n"
        f"Options source  : {opts_src}\n"
        f"Futures source  : {futs_src}\n"
        f"Shadow meta     : {shadow_meta.get('when_utc','NA')}"
    ))
    txt.append(_section_txt("AUTO (Top 5 curated — paper)",
        f"Stats: trades={auto_stats['trades']} approx_winrate={auto_stats['approx_winrate']} approx_pnl={auto_stats['approx_pnl']}\n" +
        _fmt_rows(auto_eq, ["Timestamp","Symbol","Entry","SL","Target","proba","Reason"], 20)
    ))
    txt.append(_section_txt("ALGO LAB (Exploratory — paper, NOT messaged)",
        f"Stats: trades={algo_stats['trades']} approx_winrate={algo_stats['approx_winrate']} approx_pnl={algo_stats['approx_pnl']}\n" +
        _fmt_rows(algo_eq, ["Timestamp","Symbol","Entry","SL","Target","proba","Reason"], 20)
    ))
    txt.append(_section_txt("DERIVATIVES — OPTIONS (paper)", 
        _fmt_rows(opts, ["Timestamp","Symbol","Leg","Strike","Expiry","EntryPrice","SL","Target","RR","Reason"], 20)
    ))
    txt.append(_section_txt("DERIVATIVES — FUTURES (paper)", 
        _fmt_rows(futs, ["Timestamp","Symbol","Expiry","EntryPrice","SL","Target","Lots","Reason"], 20)
    ))
    # SHADOW
    wf_syms = len(shadow_wf or {})
    wf_avg = "n/a"
    if wf_syms:
        try:
            wf_avg = sum(v.get("hit_rate",0) for v in shadow_wf.values())/wf_syms
            wf_avg = f"{wf_avg:.2f}"
        except Exception: pass
    dr_syms = len(shadow_drift or {})
    dr_neg = 0
    if dr_syms:
        try:
            dr_neg = sum(1 for v in shadow_drift.values() if v.get("delta",0) < -0.01)
        except Exception: pass
    txt.append(_section_txt("SHADOW LAB (WF/Drift/Robust warmup)",
        f"WF symbols={wf_syms}, avg hit-rate={wf_avg}\n"
        f"Drift symbols={dr_syms}, negatives(<-1%)={dr_neg}\n"
        f"Meta: {json.dumps(shadow_meta, indent=2)[:400]}..."
    ))
    # DL
    txt.append(_section_txt("SHADOW — Deep Learning (GRU)",
        json.dumps(dl_eval, indent=2)[:600] + "..."
    ))

    txt_path = os.path.join(RDIR, "eod_report.txt")
    open(txt_path, "w").write("\n".join(txt))

    # HTML
    html = [f"<h2>NIFTY500 Pro Pro — EOD God Report  ({TS})</h2>"]
    html.append(f"<h3>SOURCES</h3><pre>{json.dumps(sources, indent=2)}</pre>")
    html.append(_html_table(auto_eq, ["Timestamp","Symbol","Entry","SL","Target","proba","Reason"], "AUTO (Top 5 curated — paper)"))
    html.append(_html_table(algo_eq, ["Timestamp","Symbol","Entry","SL","Target","proba","Reason"], "ALGO LAB (Exploratory — paper, NOT messaged)"))
    html.append(_html_table(opts, ["Timestamp","Symbol","Leg","Strike","Expiry","EntryPrice","SL","Target","RR","Reason"], "DERIVATIVES — OPTIONS (paper)"))
    html.append(_html_table(futs, ["Timestamp","Symbol","Expiry","EntryPrice","SL","Target","Lots","Reason"], "DERIVATIVES — FUTURES (paper)"))
    html.append(f"<h3>SHADOW LAB</h3><p>WF symbols={wf_syms}, avg hit-rate={wf_avg} — Drift symbols={dr_syms}, negatives={dr_neg}</p>")
    html.append(f"<details><summary>DL (GRU) eval</summary><pre>{json.dumps(dl_eval, indent=2)}</pre></details>")
    html_path = os.path.join(RDIR, "eod_report.html")
    open(html_path, "w").write("\n".join(html))

    return {"txt": txt_path, "html": html_path}
