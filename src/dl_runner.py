# src/dl_runner.py
from __future__ import annotations
import os, json, time, datetime as dt
from typing import Dict, Tuple
import pandas as pd
import numpy as np

try:
    import torch
    from torch.utils.data import Dataset, DataLoader
    from dl_models.temporal import SmallGRU
except Exception:
    torch = None
    SmallGRU = None

# Paths
MODELS_DIR   = "models"
REPORTS_DIR  = "reports/shadow"
META_PATH    = os.path.join(MODELS_DIR, "dl_meta.json")
CKPT_PATH    = os.path.join(MODELS_DIR, "dl_small_gru.pth")
EVAL_PATH    = os.path.join(REPORTS_DIR, "dl_eval.json")

# Hyperparams
SEQ_LEN   = 120       # sequence length (e.g., last 120 hours)
HORIZON   = 5         # predict 5-hour ahead move
MAX_SYMBOLS = 30      # cap symbols for speed
EPOCHS    = 2         # short training, avoid long runs
BATCH     = 64
LR        = 1e-3
TRAIN_MINUTES_CAP = 3 # max training time per run (minutes)

# Readiness thresholds (Kill-switch)
READY_THRESH = {
    "min_symbols": 10,
    "min_epochs": 2,
    "hit_rate": 0.55,   # require ≥55% hit-rate
    "brier_max": 0.25   # require ≤0.25 Brier score
}

# --- Helpers -----------------------------------------------------

def _ensure_dirs():
    os.makedirs(MODELS_DIR, exist_ok=True)
    os.makedirs(REPORTS_DIR, exist_ok=True)

def _load_hourly_table() -> pd.DataFrame:
    """Try loading hourly or daily fallback parquet/csv from datalake."""
    candidates = [
        "datalake/hourly_equity.parquet",
        "datalake/hourly_equity.csv",
        "datalake/daily_equity.parquet",
        "datalake/daily_equity.csv",
    ]
    for p in candidates:
        if os.path.exists(p):
            try:
                if p.endswith(".parquet"):
                    return pd.read_parquet(p)
                return pd.read_csv(p)
            except Exception:
                continue
    return pd.DataFrame()

def _prep_panel(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Group dataframe into per-symbol sorted panels."""
    need = {"Symbol","Date","Open","High","Low","Close","Volume"}
    if df.empty or not need.issubset(df.columns):
        return {}
    df = df.copy()
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.sort_values(["Symbol","Date"])
    return {s: g.reset_index(drop=True) for s, g in df.groupby("Symbol")}

def _make_seq_features(g: pd.DataFrame, seq_len=SEQ_LEN):
    """
    Simple engineered features per bar + forward label.
    Label = future return over HORIZON > 0.
    """
    p = g.copy()
    p["ret"]   = p["Close"].pct_change().fillna(0.0)
    p["ema12"] = p["Close"].ewm(span=12, adjust=False).mean()
    p["ema26"] = p["Close"].ewm(span=26, adjust=False).mean()
    p["ema_d"] = (p["ema12"] - p["ema26"]) / (p["Close"] + 1e-9)
    p["rng"]   = (p["High"] - p["Low"]) / (p["Close"] + 1e-9)
    p["zv"]    = (p["Volume"] - p["Volume"].rolling(20).mean()) / (p["Volume"].rolling(20).std() + 1e-9)
    p = p.dropna().reset_index(drop=True)

    # label
    y = (p["Close"].shift(-HORIZON) / p["Close"] - 1.0).fillna(0.0)
    y = (y > 0).astype(np.float32)

    feats = p[["ret","ema_d","rng","zv"]].values.astype(np.float32)
    X, Y = [], []
    for i in range(len(feats) - seq_len - HORIZON):
        X.append(feats[i:i+seq_len])
        Y.append(y.iloc[i+seq_len])
    if not X:
        return np.zeros((0, seq_len, 4), np.float32), np.zeros((0,), np.float32)
    return np.stack(X), np.array(Y)

class PanelDS(Dataset):
    def __init__(self, panel: Dict[str, pd.DataFrame], take_syms=MAX_SYMBOLS):
        self.samples = []
        syms = list(panel.keys())[:take_syms]
        for s in syms:
            X, Y = _make_seq_features(panel[s])
            if len(X):
                self.samples.append((s, X, Y))
        self.index = []
        for si, (_, X, Y) in enumerate(self.samples):
            for j in range(len(X)):
                self.index.append((si, j))

    def __len__(self): return len(self.index)
    def __getitem__(self, i):
        si, j = self.index[i]
        s, X, Y = self.samples[si]
        return X[j], Y[j]

def _brier(p, y): return float(np.mean((np.clip(p,1e-6,1-1e-6) - y)**2))
def _hit_rate(p, y):
    picks = (p >= 0.5).astype(int)
    return float((picks == y).mean()) if len(y) else 0.0

# --- Shadow Cycle ------------------------------------------------

def shadow_cycle(train_minutes_cap: int = TRAIN_MINUTES_CAP):
    """
    Train tiny GRU briefly and evaluate; write EVAL_PATH and META_PATH.
    """
    _ensure_dirs()
    if (torch is None) or (SmallGRU is None):
        json.dump({"status":"no_torch"}, open(EVAL_PATH,"w"), indent=2)
        json.dump({"ready": False, "reason":"torch_missing"}, open(META_PATH,"w"), indent=2)
        return {"status":"no_torch"}

    df = _load_hourly_table()
    panel = _prep_panel(df)
    ds = PanelDS(panel, take_syms=MAX_SYMBOLS)
    if len(ds) < READY_THRESH["min_symbols"]:
        meta = {"ready": False, "reason":"not_enough_symbols", "symbols": len(ds)}
        json.dump(meta, open(META_PATH,"w"), indent=2)
        json.dump({"status":"no_data", "symbols": len(ds)}, open(EVAL_PATH,"w"), indent=2)
        return {"status":"no_data", "symbols": len(ds)}

    # train/test split chronologically
    n = len(ds)
    idx = np.arange(n)
    split = int(n * 0.8)
    tr_idx, te_idx = idx[:split], idx[split:]

    class _Subset(PanelDS):
        def __init__(self, base, id_):
            self.base, self.id_ = base, id_
        def __len__(self): return len(self.id_)
        def __getitem__(self, i): return self.base[self.id_[i]]

    train_loader = DataLoader(_Subset(ds, tr_idx), batch_size=BATCH, shuffle=True)
    test_loader  = DataLoader(_Subset(ds, te_idx), batch_size=BATCH, shuffle=False)

    device = "cpu"
    model = SmallGRU(n_features=4, hidden=32, dropout=0.1).to(device)
    opt = torch.optim.Adam(model.parameters(), lr=LR)
    bce = torch.nn.BCELoss()

    t0 = time.time()
    epochs_done = 0
    for epoch in range(EPOCHS):
        model.train()
        for X, Y in train_loader:
            X = X.to(device)
            Y = Y.to(device)
            opt.zero_grad()
            out = model(X)
            loss = bce(out, Y)
            loss.backward()
            opt.step()
        epochs_done += 1
        if time.time() - t0 > train_minutes_cap * 60:
            break

    # eval
    model.eval()
    preds, truth = [], []
    with torch.no_grad():
        for X, Y in test_loader:
            X = X.to(device)
            out = model(X).cpu().numpy()
            preds.append(out)
            truth.append(Y.numpy())
    if not preds:
        eval_res = {"status":"eval_empty"}
    else:
        p = np.concatenate(preds)
        y = np.concatenate(truth)
        eval_res = {
            "status": "ok",
            "epochs": epochs_done,
            "hit_rate": round(_hit_rate(p, y), 4),
            "brier": round(_brier(p, y), 4),
            "n_test": int(len(y))
        }

    torch.save(model.state_dict(), CKPT_PATH)
    json.dump(eval_res, open(EVAL_PATH,"w"), indent=2)

    ready = (
        eval_res.get("status") == "ok" and
        eval_res.get("epochs", 0) >= READY_THRESH["min_epochs"] and
        eval_res.get("hit_rate", 0) >= READY_THRESH["hit_rate"] and
        eval_res.get("brier", 1) <= READY_THRESH["brier_max"]
    )
    meta = {
        "ready": bool(ready),
        "evaluated_at": dt.datetime.utcnow().isoformat()+"Z",
        "thresholds": READY_THRESH,
        "eval": eval_res
    }
    json.dump(meta, open(META_PATH,"w"), indent=2)
    return meta

# --- Prediction --------------------------------------------------

def predict_topk_if_ready(top_k=5) -> Tuple[pd.DataFrame, str]:
    """
    If DL is ready, score recent sequences and return top-k suggestions.
    Else returns (empty_df, reason).
    """
    try:
        meta = json.load(open(META_PATH))
    except Exception:
        return pd.DataFrame(), "dl_not_ready"
    if not meta.get("ready", False):
        return pd.DataFrame(), "dl_not_ready"

    if (torch is None) or (SmallGRU is None):
        return pd.DataFrame(), "dl_torch_missing"

    df = _load_hourly_table()
    panel = _prep_panel(df)
    if not panel:
        return pd.DataFrame(), "dl_no_data"

    device = "cpu"
    model = SmallGRU(n_features=4, hidden=32, dropout=0.0).to(device)
    try:
        model.load_state_dict(torch.load(CKPT_PATH, map_location=device))
    except Exception:
        return pd.DataFrame(), "dl_ckpt_missing"
    model.eval()

    rows = []
    with torch.no_grad():
        for s, g in panel.items():
            X, _ = _make_seq_features(g)
            if len(X) == 0: continue
            xt = torch.from_numpy(X[-1:]).to(device)
            p = float(model(xt).cpu().numpy().ravel()[0])
            last = g.iloc[-1]
            close = float(last["Close"])
            entry = close
            sl    = close * 0.99
            tgt   = close * 1.01
            rows.append({"Symbol": s, "Entry": entry, "SL": sl,
                         "Target": tgt, "proba": p, "Reason": "DL-GRU"})

    if not rows:
        return pd.DataFrame(), "dl_scored_none"

    dfp = pd.DataFrame(rows).sort_values("proba", ascending=False).head(top_k)
    return dfp, "dl_ready"
