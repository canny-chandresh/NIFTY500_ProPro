# src/telegram.py
"""
Telegram utilities:
- send_text(): send a Markdown-formatted message (auto-splits long text)
- format_trades_block(): pretty block for AUTO / ALGO / AI picks, with icons & win % (proba)
- build_recs_message(): compose a full message for AUTO/ALGO/AI in one go
- send_recommendations(): convenience wrapper to format & send in one call
- send_stats(): send compact stats (e.g., win-rate, sharpe) for AUTO/ALGO

Requires environment variables:
  TG_BOT_TOKEN, TG_CHAT_ID
"""

from __future__ import annotations
import os
import math
import time
import json
import html
import requests
import pandas as pd

# ---------- Telegram basics ----------

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "").strip()
TG_CHAT_ID   = os.getenv("TG_CHAT_ID", "").strip()
TG_URL       = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage" if TG_BOT_TOKEN else None

# Telegram allows ~4096 chars; keep margin for safety
_TG_MAX_CHARS = 4000

def _md_escape(s: str) -> str:
    """
    Escape MarkdownV2 special chars conservatively so symbols/tickers don't break formatting.
    We use *basic* Markdown (not V2) in this project, but escaping some symbols still helps.
    """
    if s is None:
        return ""
    # Basic sanitize: avoid accidental markdown issues; also strip control chars
    bad = ['`', '_', '*', '[', ']', '(', ')', '~', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
    out = str(s)
    for ch in bad:
        out = out.replace(ch, f"\\{ch}")
    return out

def _chunk_message(text: str, limit: int = _TG_MAX_CHARS):
    """Yield chunks <= limit; try to split on line breaks."""
    if not text:
        return []
    if len(text) <= limit:
        return [text]
    lines = text.split("\n")
    buf, cur = [], 0
    tmp = []
    for ln in lines:
        if cur + len(ln) + 1 > limit:
            buf.append("\n".join(tmp))
            tmp, cur = [ln], len(ln) + 1
        else:
            tmp.append(ln)
            cur += len(ln) + 1
    if tmp:
        buf.append("\n".join(tmp))
    return buf

def send_text(text: str, parse_mode: str = "Markdown", disable_web_page_preview: bool = True) -> None:
    """
    Send a Telegram message. If token/chat not present, print to stdout instead.
    Splits long messages automatically.
    """
    if not text:
        return
    chunks = _chunk_message(text)
    if not TG_URL or not TG_CHAT_ID:
        print("[TELEGRAM simulate] (no TG_BOT_TOKEN/TG_CHAT_ID)")
        for i, c in enumerate(chunks, 1):
            print(f"\n--- chunk {i}/{len(chunks)} ---\n{c}")
        return

    for i, c in enumerate(chunks, 1):
        try:
            resp = requests.post(
                TG_URL,
                data={
                    "chat_id": TG_CHAT_ID,
                    "text": c,
                    "parse_mode": parse_mode,
                    "disable_web_page_preview": "true" if disable_web_page_preview else "false",
                },
                timeout=20,
            )
            if resp.status_code != 200:
                print(f"[TELEGRAM] HTTP {resp.status_code}: {resp.text[:200]}")
            # politeness delay to avoid rate limits
            time.sleep(0.3)
        except Exception as e:
            print(f"[TELEGRAM] send error: {e}")

# ---------- Formatting helpers ----------

# Icons per mode/type for easy scanning
ICON_BY_MODE = {
    "swing": "📈",
    "intraday": "🏃",
    "futures": "📊",
    "options": "⚙️",
}
# Fallback if mode missing/unknown
DEFAULT_ICON = "📈"

def _row_icon(mode: str | None) -> str:
    if not mode:
        return DEFAULT_ICON
    m = str(mode).strip().lower()
    return ICON_BY_MODE.get(m, DEFAULT_ICON)

def _fmt_pct(x: float, digits: int = 1) -> str:
    try:
        return f"{x*100:.{digits}f}%"
    except Exception:
        return "—"

def _fmt_price(x) -> str:
    try:
        return f"{float(x):.2f}"
    except Exception:
        return "—"

def format_trades_block(df: pd.DataFrame, title: str) -> str:
    """
    Build a Markdown block for a list of trades with icons and per-trade win probability (proba).
    Expects columns: Symbol, Entry, Target, SL, proba, [mode]
    """
    esc_title = _md_escape(title)
    if df is None or df.empty:
        return f"*{esc_title}*: (no trades)\n"

    lines = [f"*{esc_title}*: {len(df)} picks"]
    cols = df.columns.str.lower().tolist()
    for _, r in df.iterrows():
        sym  = _md_escape(str(r.get("Symbol", "")))
        mode = str(r.get("mode", "swing")).lower() if "mode" in cols else "swing"
        icon = _row_icon(mode)
        entry = _fmt_price(r.get("Entry", r.get("fill_price")))
        tgt   = _fmt_price(r.get("Target"))
        sl    = _fmt_price(r.get("SL"))
        proba = r.get("proba", None)
        ptxt  = _fmt_pct(float(proba)) if proba is not None else "—"
        reason = _md_escape(str(r.get("Reason","")))
        # Example line:
        # 📈 *RELIANCE* | swing | Buy 2500.00 | TP 2600.00 | SL 2450.00 | p=62.3% | reason: BREAKOUT
        line = f"{icon} *{sym}* | {mode} | Buy {entry} | TP {tgt} | SL {sl} | p={ptxt}"
        if reason:
            line += f" | {reason}"
        lines.append(line)

    return "\n".join(lines) + "\n"

def build_recs_message(auto_df: pd.DataFrame | None = None,
                       algo_df: pd.DataFrame | None = None,
                       ai_df:   pd.DataFrame | None = None,
                       header:  str | None = None) -> str:
    """
    Build a single message that includes AUTO, ALGO, and AI blocks.
    """
    parts = []
    if header:
        parts.append(f"*{_md_escape(header)}*")
    if auto_df is not None:
        parts.append(format_trades_block(auto_df, "AUTO (Top Picks)"))
    if algo_df is not None:
        parts.append(format_trades_block(algo_df, "ALGO (Exploration)"))
    if ai_df is not None and not (ai_df is auto_df or ai_df is algo_df):
        parts.append(format_trades_block(ai_df, "AI (Policy Decisions)"))
    return "\n".join([p for p in parts if p])

def send_recommendations(auto_df: pd.DataFrame | None = None,
                         algo_df: pd.DataFrame | None = None,
                         ai_df:   pd.DataFrame | None = None,
                         header:  str | None = None,
                         parse_mode: str = "Markdown") -> None:
    """
    Convenience wrapper to format and send recommendations in one shot.
    """
    msg = build_recs_message(auto_df, algo_df, ai_df, header)
    send_text(msg, parse_mode=parse_mode)

# ---------- Stats / summaries ----------

def send_stats(stats: dict, title: str = "Performance Summary") -> None:
    """
    Send a compact performance summary (e.g., win-rate, sharpe) for AUTO and ALGO.
    Expected `stats` shape:
      {"AUTO": {"win_rate": 0.61, "sharpe": 0.9, "max_drawdown": 0.08},
       "ALGO": {...}}
    """
    def _val(d, k, default="—"):
        try:
            v = float(d.get(k))
            if k == "win_rate":
                return f"{v:.2f}"
            elif k == "sharpe":
                return f"{v:.2f}"
            elif k == "max_drawdown":
                return f"{v:.2f}"
            return f"{v}"
        except Exception:
            return default

    a = stats.get("AUTO", {})
    g = stats.get("ALGO", {})
    lines = [
        f"*{_md_escape(title)}*",
        f"AUTO → WR: {_val(a,'win_rate')} | Sharpe: {_val(a,'sharpe')} | DD: {_val(a,'max_drawdown')}",
        f"ALGO → WR: {_val(g,'win_rate')} | Sharpe: {_val(g,'sharpe')} | DD: {_val(g,'max_drawdown')}",
    ]
    send_text("\n".join(lines))

# ---------- Optional: quick test ----------

if __name__ == "__main__":
    # Dry run demo (no TG creds needed)
    demo = pd.DataFrame([
        {"Symbol":"RELIANCE","Entry":2500,"Target":2600,"SL":2450,"proba":0.623,"mode":"swing","Reason":"BREAKOUT"},
        {"Symbol":"HDFCBANK","Entry":1650,"Target":1690,"SL":1620,"proba":0.571,"mode":"intraday","Reason":"VWAP PULLBACK"},
        {"Symbol":"BANKNIFTY","Entry":49200,"Target":49800,"SL":48950,"proba":0.553,"mode":"futures","Reason":"TREND CONT"},
        {"Symbol":"TCS","Entry":3900,"Target":3990,"SL":3840,"proba":0.585,"mode":"swing","Reason":"EMA20 BOUNCE"},
        {"Symbol":"INFY","Entry":1600,"Target":1648,"SL":1560,"proba":0.602,"mode":"options","Reason":"OI BUILDUP"},
    ])
    send_recommendations(demo.head(5), demo.iloc[2:4], header="Demo Picks")
    send_stats({"AUTO":{"win_rate":0.61,"sharpe":0.92,"max_drawdown":0.07},
                "ALGO":{"win_rate":0.54,"sharpe":0.55,"max_drawdown":0.11}},
               title="Demo Stats")
