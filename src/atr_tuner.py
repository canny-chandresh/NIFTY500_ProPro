# src/atr_tuner.py
from __future__ import annotations
import os, json, datetime as dt
from typing import Dict, Tuple

STATE = "reports/metrics/atr_tuner_state.json"

BOUNDS = {
    "intraday": {"tp": (1.0, 2.5), "sl": (0.5, 1.5)},
    "swing":    {"tp": (2.0, 4.5), "sl": (1.0, 2.0)},
    "futures":  {"tp": (1.5, 3.0), "sl": (0.7, 1.5)},
    "options":  {"tp": (None, None), "sl": (None, None)},
}

STEP = {"tp": 0.10, "sl": 0.05}

PRESETS = {
    "calm":   {"intraday": (1.6, 0.8), "swing": (2.6, 1.2), "futures": (2.0, 1.0)},
    "normal": {"intraday": (1.8, 0.9), "swing": (3.0, 1.5), "futures": (2.2, 1.1)},
    "volatile":{"intraday": (2.2, 1.1), "swing": (3.6, 1.8), "futures": (2.6, 1.3)},
}

def _read() -> Dict:
    if os.path.exists(STATE):
        try: return json.load(open(STATE))
        except Exception: pass
    return {"versions": {}, "history": []}

def _write(st: Dict):
    os.makedirs(os.path.dirname(STATE) or ".", exist_ok=True)
    json.dump(st, open(STATE, "w"), indent=2)

def _ctx_bucket(vix: float|None, regime: str) -> str:
    r = (regime or "neutral").lower()
    if vix is None: return "normal"
    if vix <= 12 and r in ("bull","neutral"): return "calm"
    if vix >= 18 or r == "bear": return "volatile"
    return "normal"

def _bounded(val: float, lo: float, hi: float) -> float:
    return float(max(lo, min(hi, val)))

def _ensure_mode(st: Dict, mode: str, bucket: str):
    if mode not in st["versions"]:
        base = PRESETS.get(bucket, PRESETS["normal"]).get(mode, (None, None))
        st["versions"][mode] = {
            "tp_atr": None if base[0] is None else float(base[0]),
            "sl_atr": None if base[1] is None else float(base[1]),
            "when_utc": dt.datetime.utcnow().isoformat()+"Z",
            "ctx_bucket": bucket
        }

def get_multipliers(mode: str, ctx: Dict) -> Tuple[float|None, float|None]:
    if mode == "options": return (None, None)
    st = _read()
    vix = ctx.get("vix", None)
    regime = (ctx.get("regime") or "neutral")
    bucket = _ctx_bucket(vix, regime)
    _ensure_mode(st, mode, bucket)

    rec = st["versions"][mode]
    desired = PRESETS.get(bucket, PRESETS["normal"]).get(mode, (rec["tp_atr"], rec["sl_atr"]))
    tp = rec["tp_atr"] if rec["tp_atr"] is not None else desired[0]
    sl = rec["sl_atr"] if rec["sl_atr"] is not None else desired[1]

    if rec.get("ctx_bucket") != bucket and all(x is not None for x in desired):
        tp = 0.7*tp + 0.3*desired[0]
        sl = 0.7*sl + 0.3*desired[1]
        rec["ctx_bucket"] = bucket

    b = BOUNDS.get(mode, BOUNDS["swing"])
    if all(v is not None for v in (tp, sl, b["tp"][0], b["tp"][1], b["sl"][0], b["sl"][1])):
        tp = _bounded(tp, b["tp"][0], b["tp"][1])
        sl = _bounded(sl, b["sl"][0], b["sl"][1])

    rec["tp_atr"] = tp
    rec["sl_atr"] = sl
    rec["when_utc"] = dt.datetime.utcnow().isoformat()+"Z"
    st["versions"][mode] = rec
    _write(st)
    return tp, sl

def update_from_metrics(ctx: Dict):
    try:
        from metrics_tracker import summarize_last_n
        metr = summarize_last_n(days=10)
    except Exception:
        return

    st = _read()
    vix = ctx.get("vix", None)
    regime = (ctx.get("regime") or "neutral")
    bucket = _ctx_bucket(vix, regime)

    def _avg(a, b, key):
        return 0.5*float(a.get(key, 0.0)) + 0.5*float(b.get(key, 0.0))

    auto = metr.get("AUTO", {})
    algo = metr.get("ALGO", {})
    merged = {
        "win_rate": _avg(auto, algo, "win_rate"),
        "sharpe": _avg(auto, algo, "sharpe"),
        "max_drawdown": _avg(auto, algo, "max_drawdown")
    }

    def _nudges(stats):
        wr = float(stats.get("win_rate", 0.0))
        sharpe = float(stats.get("sharpe", 0.0))
        dd = float(stats.get("max_drawdown", 0.0))
        d_tp = 0.0; d_sl = 0.0
        if wr >= 0.58 and sharpe >= 0.7 and dd <= 0.08:
            d_tp += STEP["tp"]
        elif 0.48 <= wr < 0.58 or dd > 0.10:
            d_tp -= STEP["tp"] * 0.5
            d_sl += STEP["sl"] * 0.5
        if wr < 0.45 or dd > 0.15 or sharpe < 0.2:
            d_tp -= STEP["tp"]
            d_sl += STEP["sl"]
        return d_tp, d_sl

    d_tp, d_sl = _nudges(merged)

    for mode in ("intraday","swing","futures"):
        _ensure_mode(st, mode, bucket)
        rec = st["versions"][mode]
        tp, sl = rec["tp_atr"], rec["sl_atr"]
        if tp is None or sl is None:
            continue
        tp += d_tp; sl += d_sl
        b = BOUNDS[mode]
        tp = _bounded(tp, b["tp"][0], b["tp"][1])
        sl = _bounded(sl, b["sl"][0], b["sl"][1])
        rec["tp_atr"] = float(tp); rec["sl_atr"] = float(sl)
        rec["when_utc"] = dt.datetime.utcnow().isoformat()+"Z"
        st["versions"][mode] = rec

    st["history"].append({
        "when_utc": dt.datetime.utcnow().isoformat()+"Z",
        "ctx_bucket": bucket,
        "merged_wr": merged["win_rate"],
        "merged_sharpe": merged["sharpe"],
        "merged_dd": merged["max_drawdown"],
        "d_tp": d_tp, "d_sl": d_sl
    })
    st["history"] = st["history"][-200:]
    _write(st)
