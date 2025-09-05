from __future__ import annotations
import os, json, datetime as dt
import pandas as pd

TS = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
RDIR = "reports"

def _load_csv(path: str) -> pd.DataFrame:
    try:
        if os.path.exists(path):
            df = pd.read_csv(path)
            return df
    except Exception:
        pass
    return pd.DataFrame()

def _load_json(path: str):
    try:
        if os.path.exists(path):
            return json.load(open(path))
    except Exception:
        pass
    return {}

def _section_txt(title: str, body: str) -> str:
    return f"\n===== {title} =====\n{body.strip()}\n"

def _fmt_rows(df: pd.DataFrame, cols: list[str], max_rows: int = 20) -> str:
    if df is None or df.empty:
        return "(none)"
    d = df[cols].head(max_rows).copy()
    return d.to_string(index=False)

def _html_table(df: pd.DataFrame, cols: list[str], caption: str = "", max_rows: int = 30) -> str:
    if df is None or df.empty:
        return f"<h3>{caption}</h3><p>(none)</p>"
    d = df[cols].head(max_rows).copy()
    return f"<h3>{caption}</h3>" + d.to_html(index=False, escape=True)

def _read_text(path: str) -> str:
    try:
        if os.path.exists(path):
            return open(path).read().strip()
    except Exception:
        pass
    return "NA"

def build_eod():
    os.makedirs(RDIR, exist_ok=True)

    # === Load data ===
    auto_eq = _load_csv("datalake/paper_trades.csv")           # AUTO
    algo_eq = _load_csv("datalake/algo_paper.csv")             # ALGO Lab (exploratory)
    opts    = _load_csv("datalake/options_paper.csv")
    futs    = _load_csv("datalake/futures_paper.csv")

    shadow_wf   = _load_json("reports/shadow/walkforward_summary.json")
    shadow_drift= _load_json("reports/shadow/drift_snapshot.json")
    shadow_meta = _load_json("reports/shadow/_shadow_run.json")

    sources = _load_json("reports/sources_used.json")
    opts_src = _read_text("reports/options_source.txt")
    futs_src = _read_text("reports/futures_source.txt")

    # === Compute quick stats ===
    def _pnl_stats(df, entry="Entry", tgt="Target", sl="SL"):
        if df is None or df.empty: 
            return {"trades":0, "approx_winrate":"n/a"}
        wins = (df[tgt] > df[entry]).sum()
        return {"trades": int(len(df)), "approx_winrate": f"{wins/max(1,len(df))*100:.0f}%"}

    auto_stats = _pnl_stats(auto_eq)
    algo_stats = _pnl_stats(algo_eq)

    # === TEXT REPORT ===
    txt = []
    txt.append(f"NIFTY500 Pro Pro — EOD God Report  ({TS})\n")

    # Sources
    txt.append(_section_txt("SOURCES",
        f"Equity source   : {sources.get('equities',{}).get('equities_source','unknown')} "
        f"({sources.get('equities',{}).get('rows','?')} rows)\n"
        f"Options source  : {opts_src}\n"
        f"Futures source  : {futs_src}\n"
        f"Shadow meta     : {shadow_meta.get('when_utc','NA')}"
    ))

    # AUTO
    txt.append(_section_txt("AUTO (Top 5 curated — paper)",
        f"Stats: trades={auto_stats['trades']} approx_winrate={auto_stats['approx_winrate']}\n" +
        _fmt_rows(auto_eq, ["Timestamp","Symbol","Entry","SL","Target","proba","Reason"], max_rows=20)
    ))

    # ALGO
    txt.append(_section_txt("ALGO LAB (Exploratory — paper, NOT messaged)",
        f"Stats: trades={algo_stats['trades']} approx_winrate={algo_stats['approx_winrate']}\n" +
        _fmt_rows(algo_eq, ["Timestamp","Symbol","Entry","SL","Target","proba","Reason"], max_rows=20)
    ))

    # Options / Futures
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
        except Exception:
            pass
    dr_syms = len(shadow_drift or {})
    dr_neg = 0
    if dr_syms:
        try:
            dr_neg = sum(1 for v in shadow_drift.values() if v.get("delta",0) < -0.01)
        except Exception:
            pass
    txt.append(_section_txt("SHADOW LAB (WF/Drift/Robust warmup)",
        f"WF symbols={wf_syms}, avg hit-rate={wf_avg}\n"
        f"Drift symbols={dr_syms}, negatives(<-1%)={dr_neg}\n"
        f"Meta: {json.dumps(shadow_meta, indent=2)[:400]}..."
    ))

    # save TXT
    txt_path = os.path.join(RDIR, "eod_report.txt")
    open(txt_path, "w").write("\n".join(txt))

    # === HTML REPORT ===
    html = [f"<h2>NIFTY500 Pro Pro — EOD God Report  ({TS})</h2>"]
    html.append(f"<h3>SOURCES</h3><pre>{json.dumps(sources, indent=2)}</pre>")
    html.append(_html_table(auto_eq, ["Timestamp","Symbol","Entry","SL","Target","proba","Reason"], "AUTO (Top 5 curated — paper)"))
    html.append(_html_table(algo_eq, ["Timestamp","Symbol","Entry","SL","Target","proba","Reason"], "ALGO LAB (Exploratory — paper, NOT messaged)"))
    html.append(_html_table(opts, ["Timestamp","Symbol","Leg","Strike","Expiry","EntryPrice","SL","Target","RR","Reason"], "DERIVATIVES — OPTIONS (paper)"))
    html.append(_html_table(futs, ["Timestamp","Symbol","Expiry","EntryPrice","SL","Target","Lots","Reason"], "DERIVATIVES — FUTURES (paper)"))
    html.append(f"<h3>SHADOW LAB</h3><p>WF symbols={wf_syms}, avg hit-rate={wf_avg} — Drift symbols={dr_syms}, negatives={dr_neg}</p>")
    html.append(f"<details><summary>Shadow meta</summary><pre>{json.dumps(shadow_meta, indent=2)}</pre></details>")
    html_path = os.path.join(RDIR, "eod_report.html")
    open(html_path, "w").write("\n".join(html))

    return {"txt": txt_path, "html": html_path}
