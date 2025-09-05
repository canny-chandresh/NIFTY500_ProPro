from __future__ import annotations
import os, json, time, datetime as dt
from typing import Dict, Tuple, List
import numpy as np
import pandas as pd

from config import CONFIG

try:
    import torch
    from torch.utils.data import Dataset, DataLoader
    from dl_models.temporal import SmallGRU
except Exception:
    torch = None
    SmallGRU = None

DL_DIR = "datalake"
MODELS_DIR = "models"
REPORTS_DIR = "reports/shadow"
META_PATH   = os.path.join(MODELS_DIR, "dl_meta.json")
CKPT_PATH   = os.path.join(MODELS_DIR, "dl_small_gru.pth")
EVAL_PATH   = os.path.join(REPORTS_DIR, "dl_eval.json")
EVAL_HIST   = os.path.join(REPORTS_DIR, "dl_eval_history.json")

def _ensure_dirs():
    os.makedirs(DL_DIR, exist_ok=True)
    os.makedirs(MODELS_DIR, exist_ok=True)
    os.makedirs(REPORTS_DIR, exist_ok=True)

def _load_feat_table() -> pd.DataFrame:
    p = os.path.join(DL_DIR,"features_hourly.parquet")
    if os.path.exists(p):
        try: return pd.read_parquet(p)
        except Exception: pass
    return pd.DataFrame()

def _select_columns(df: pd.DataFrame):
    cols = ["ret_1","ret_5","ret_20","ema_diff","rng","vol_z20","gap_pct","vix_norm","gift_norm","news_sent_1h"]
    present = [c for c in cols if c in df.columns]
    # Make at least 8 features
    while len(present) < 8:
        present.append("ret_1")
    return present[:8]

def _make_sequences(df: pd.DataFrame, seq_len: int, horizon_h: int, max_symbols: int):
    df = df.sort_values(["Symbol","Datetime"])
    feats_cols = _select_columns(df)
    Xs, Ys = [], []
    sym_index = []
    for s, g in df.groupby("Symbol"):
        g = g.reset_index(drop=True)
        # must have label column
        lab_col = f"label_up_{horizon_h}h"
        if lab_col not in g.columns: 
            continue
        F = g[feats_cols].astype(np.float32).values
        y = g[lab_col].astype(np.float32).values
        if len(F) <= seq_len + horizon_h + 5: 
            continue
        for i in range(len(F) - seq_len - horizon_h):
            Xs.append(F[i:i+seq_len])
            Ys.append(y[i+seq_len])
            sym_index.append(s)
        if len(sym_index) > max_symbols * 200:  # cap for runner time
            break
    if not Xs:
        return np.zeros((0, seq_len, len(feats_cols)), np.float32), np.zeros((0,), np.float32), []
    X = np.stack(Xs); Y = np.array(Ys)
    return X, Y, sym_index

class ArrDS(Dataset):
    def __init__(self, X, Y):
        self.X, self.Y = X, Y
    def __len__(self): return len(self.Y)
    def __getitem__(self, i): 
        return self.X[i], self.Y[i]

def _brier(p, y):
    p = np.clip(p, 1e-6, 1-1e-6)
    return float(np.mean((p - y)**2))

def _hit_rate(p, y):
    picks = (p >= 0.5).astype(int)
    return float((picks == y).mean()) if len(y) else 0.0

def _approx_pnl_from_returns(p, y, fut_ret):
    """
    Approx PnL per trade using forward return as proxy.
    Only count when model 'picks' (p>=0.5).
    """
    picks = (p >= 0.5)
    rets = fut_ret[picks]
    if len(rets)==0:
        return {"trades":0,"avg_ret":0.0,"sum_ret":0.0}
    return {
        "trades": int(len(rets)),
        "avg_ret": float(np.mean(rets)),
        "sum_ret": float(np.sum(rets))
    }

def shadow_cycle(train_minutes_cap: int = None):
    """
    Train tiny GRU briefly and evaluate; write EVAL_PATH and META_PATH.
    Also appends to EVAL_HIST for weekly report aggregation.
    """
    if not CONFIG.get("features",{}).get("dl_shadow", True):
        return {"status":"disabled"}

    _ensure_dirs()
    if (torch is None) or (SmallGRU is None):
        json.dump({"status":"no_torch"}, open(EVAL_PATH,"w"), indent=2)
        json.dump({"ready": False, "reason":"torch_missing"}, open(META_PATH,"w"), indent=2)
        return {"status":"no_torch"}

    seq_len    = int(CONFIG["dl"]["seq_len"])
    horizon_h  = int(CONFIG["dl"]["horizon_h"])
    max_syms   = int(CONFIG["dl"]["max_symbols"])
    epochs     = int(CONFIG["dl"]["epochs"])
    minutes_cap= int(CONFIG["dl"]["minutes_cap"] if train_minutes_cap is None else train_minutes_cap)
    thr        = CONFIG["dl"]["ready_thresholds"]

    df = _load_feat_table()
    if df.empty:
        json.dump({"status":"no_features"}, open(EVAL_PATH,"w"), indent=2)
        json.dump({"ready": False, "reason":"no_features"}, open(META_PATH,"w"), indent=2)
        return {"status":"no_features"}

    # Build sequences
    X, Y, sym_idx = _make_sequences(df, seq_len, horizon_h, max_syms)
    if len(X) < thr["min_symbols"] * 20:
        meta = {"ready": False, "reason":"not_enough_sequences", "n_seq": int(len(X))}
        json.dump(meta, open(META_PATH,"w"), indent=2)
        json.dump({"status":"no_data","n_seq": int(len(X))}, open(EVAL_PATH,"w"), indent=2)
        return {"status":"no_data","n_seq": int(len(X))}

    # Chronological split (preserve order)
    n = len(X)
    split = int(n * 0.8)
    Xtr, Ytr = X[:split], Y[:split]
    Xte, Yte = X[split:], Y[split:]

    # For PnL proxy: take forward returns from features file aligned similarly
    # (derive from one of the labels present; safest is ret_fwd_{horizon_h}h)
    lab_ret_col = f"ret_fwd_{horizon_h}h"
    # Reconstruct test forward returns by sampling same windows end points
    # We’ll approximate by taking the last rows for test window in order.
    fut = df.sort_values(["Symbol","Datetime"])
    fut_ret = fut.groupby("Symbol")[lab_ret_col].apply(lambda s: s.iloc[seq_len:]).reset_index(drop=True)
    fut_ret = fut_ret.values
    # align lengths
    fut_ret = fut_ret[-len(Y):]
    ftr, fte = fut_ret[:split], fut_ret[split:]

    # Torch
    device = "cpu"
    model = SmallGRU(n_features=X.shape[-1], hidden=48, dropout=0.1).to(device)
    opt = torch.optim.Adam(model.parameters(), lr=1e-3)
    bce = torch.nn.BCELoss()
    train_loader = DataLoader(ArrDS(torch.tensor(Xtr), torch.tensor(Ytr)), batch_size=64, shuffle=True)
    test_loader  = DataLoader(ArrDS(torch.tensor(Xte), torch.tensor(Yte)), batch_size=128, shuffle=False)

    t0 = time.time(); epochs_done = 0
    for ep in range(epochs):
        model.train()
        for xb, yb in train_loader:
            xb, yb = xb.to(device), yb.to(device)
            opt.zero_grad()
            out = model(xb)
            loss = bce(out, yb)
            loss.backward()
            opt.step()
        epochs_done += 1
        if time.time() - t0 > minutes_cap * 60:
            break

    # Eval
    model.eval()
    preds = []
    with torch.no_grad():
        for xb, yb in test_loader:
            xb = xb.to(device)
            out = model(xb).cpu().numpy()
            preds.append(out)
    if preds:
        p = np.concatenate(preds)
    else:
        p = np.zeros((len(Yte),), np.float32)

    hr = _hit_rate(p, Yte)
    br = _brier(p, Yte)
    pnl_te = _approx_pnl_from_returns(p, Yte, np.array(fte[:len(p)]))

    eval_res = {
        "status": "ok",
        "when_utc": dt.datetime.utcnow().isoformat()+"Z",
        "epochs": epochs_done,
        "n_train": int(len(Ytr)),
        "n_test": int(len(Yte)),
        "hit_rate": round(hr, 4),
        "brier": round(br, 4),
        "pnl_proxy": pnl_te
    }
    os.makedirs(REPORTS_DIR, exist_ok=True)
    json.dump(eval_res, open(EVAL_PATH,"w"), indent=2)

    # Append to history
    hist = []
    if os.path.exists(EVAL_HIST):
        try: hist = json.load(open(EVAL_HIST))
        except Exception: hist = []
    hist.append(eval_res)
    json.dump(hist[-60:], open(EVAL_HIST,"w"), indent=2)  # keep last 60 entries

    # Save checkpoint
    os.makedirs(MODELS_DIR, exist_ok=True)
    try:
        torch.save(model.state_dict(), CKPT_PATH)
    except Exception:
        pass

    ready = (
        eval_res["epochs"] >= thr["min_epochs"] and
        eval_res["hit_rate"] >= thr["hit_rate"] and
        eval_res["brier"] <= thr["brier_max"]
    )
    meta = {
        "ready": bool(ready),
        "evaluated_at": eval_res["when_utc"],
        "thresholds": thr,
        "eval": eval_res
    }
    json.dump(meta, open(META_PATH,"w"), indent=2)
    return meta

def predict_topk_if_ready(top_k=5) -> Tuple[pd.DataFrame, str]:
    try:
        meta = json.load(open(META_PATH))
    except Exception:
        return pd.DataFrame(), "dl_not_ready"
    if not meta.get("ready", False):
        return pd.DataFrame(), "dl_not_ready"

    # Score latest window for each symbol
    df = _load_feat_table()
    if df.empty:
        return pd.DataFrame(), "dl_no_data"
    df = df.sort_values(["Symbol","Datetime"]).reset_index(drop=True)

    seq_len   = int(CONFIG["dl"]["seq_len"])
    horizon_h = int(CONFIG["dl"]["horizon_h"])
    feats_cols = _select_columns(df)

    device = "cpu"
    model = SmallGRU(n_features=len(feats_cols), hidden=48, dropout=0.0).to(device)
    try:
        model.load_state_dict(torch.load(CKPT_PATH, map_location=device))
    except Exception:
        return pd.DataFrame(), "dl_ckpt_missing"
    model.eval()

    rows = []
    with torch.no_grad():
        for s, g in df.groupby("Symbol"):
            g = g.reset_index(drop=True)
            if len(g) < seq_len + horizon_h + 1: 
                continue
            X = g[feats_cols].astype(np.float32).values
            xt = torch.tensor(X[-seq_len:][None, ...]).to(device)  # (1, T, F)
            p  = float(model(xt).cpu().numpy().ravel()[0])
            last = g.iloc[-1]
            close = float(last["Close"])
            # Simple levels
            entry = close
            sl    = close * (1.0 - 0.01)   # -1%
            tgt   = close * (1.0 + 0.01)   # +1%
            rows.append({"Symbol": s, "Entry": entry, "SL": sl, "Target": tgt, "proba": p, "Reason": "DL-GRU"})

    if not rows:
        return pd.DataFrame(), "dl_scored_none"

    dfp = pd.DataFrame(rows).sort_values("proba", ascending=False).head(top_k)
    return dfp, "dl_ready"
