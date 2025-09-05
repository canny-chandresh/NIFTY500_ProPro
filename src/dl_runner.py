from __future__ import annotations
import os, json, time, datetime as dt
from typing import Dict, Tuple
import numpy as np, pandas as pd
from config import CONFIG

try:
    import torch
    from torch.utils.data import Dataset, DataLoader
    from dl_models.temporal import SmallGRU
except Exception:
    torch = None; SmallGRU = None

import dl_kill_switch as dks

DL_DIR = "datalake"; MODELS_DIR = "models"; RPT = "reports/shadow"
META_PATH = os.path.join(MODELS_DIR,"dl_meta.json")
CKPT_PATH = os.path.join(MODELS_DIR,"dl_small_gru.pth")
EVAL_PATH = os.path.join(RPT,"dl_eval.json")
EVAL_HIST = os.path.join(RPT,"dl_eval_history.json")

def _ensure_dirs():
    os.makedirs(DL_DIR, exist_ok=True); os.makedirs(MODELS_DIR, exist_ok=True); os.makedirs(RPT, exist_ok=True)

def _load_feat() -> pd.DataFrame:
    p = os.path.join(DL_DIR,"features_hourly.parquet")
    if os.path.exists(p):
        try: return pd.read_parquet(p)
        except Exception: pass
    return pd.DataFrame()

def _select_cols(df: pd.DataFrame):
    base = ["ret_1","ret_5","ret_20","ema_diff","rng","vol_z20","gap_pct","vix_norm","gift_norm","news_sent_1h"]
    have = [c for c in base if c in df.columns]
    while len(have) < 8: have.append("ret_1")
    return have[:8]

def _make_seq(df, seq_len, horizon_h, max_symbols):
    df = df.sort_values(["Symbol","Datetime"])
    cols = _select_cols(df); Xs=[]; Ys=[]
    for s, g in df.groupby("Symbol"):
        g = g.reset_index(drop=True)
        lab = f"label_up_{horizon_h}h"
        if lab not in g.columns: continue
        F = g[cols].astype(np.float32).values
        y = g[lab].astype(np.float32).values
        if len(F) <= seq_len + horizon_h + 5: continue
        for i in range(len(F) - seq_len - horizon_h):
            Xs.append(F[i:i+seq_len]); Ys.append(y[i+seq_len])
        if len(Xs) > max_symbols * 200: break
    if not Xs: return np.zeros((0, seq_len, len(cols)), np.float32), np.zeros((0,), np.float32)
    return np.stack(Xs), np.array(Ys)

class ArrDS(Dataset):
    def __init__(self, X, Y): self.X, self.Y = X, Y
    def __len__(self): return len(self.Y)
    def __getitem__(self, i): return self.X[i], self.Y[i]

def _brier(p,y): p=np.clip(p,1e-6,1-1e-6); return float(np.mean((p-y)**2))
def _hit(p,y): return float(((p>=0.5).astype(int)==y).mean()) if len(y) else 0.0

def _approx_pnl(p, y, fwd):
    picks = (p>=0.5)
    rr = fwd[picks]
    if len(rr)==0: return {"trades":0,"avg_ret":0.0,"sum_ret":0.0}
    return {"trades":int(len(rr)), "avg_ret":float(np.mean(rr)), "sum_ret":float(np.sum(rr))}

def shadow_cycle(train_minutes_cap: int = None):
    if not CONFIG["features"].get("dl_shadow", True): return {"status":"disabled"}
    _ensure_dirs()
    if (torch is None) or (SmallGRU is None):
        json.dump({"status":"no_torch"}, open(EVAL_PATH,"w"), indent=2)
        json.dump({"ready": False, "reason":"torch_missing"}, open(META_PATH,"w"), indent=2)
        return {"status":"no_torch"}

    seq_len   = int(CONFIG["dl"]["seq_len"])
    horizon_h = int(CONFIG["dl"]["horizon_h"])
    max_syms  = int(CONFIG["dl"]["max_symbols"])
    epochs    = int(CONFIG["dl"]["epochs"])
    cap_min   = int(CONFIG["dl"]["minutes_cap"] if train_minutes_cap is None else train_minutes_cap)
    thr       = CONFIG["dl"]["ready_thresholds"]

    df = _load_feat()
    if df.empty:
        json.dump({"status":"no_features"}, open(EVAL_PATH,"w"), indent=2)
        json.dump({"ready": False, "reason":"no_features"}, open(META_PATH,"w"), indent=2)
        return {"status":"no_features"}

    # sequences
    X, Y = _make_seq(df, seq_len, horizon_h, max_syms)
    if len(X) < thr["min_symbols"]*20:
        meta = {"ready": False, "reason":"not_enough_sequences", "n_seq": int(len(X))}
        json.dump(meta, open(META_PATH,"w"), indent=2)
        json.dump({"status":"no_data","n_seq": int(len(X))}, open(EVAL_PATH,"w"), indent=2)
        return {"status":"no_data","n_seq": int(len(X))}

    # chronological split
    n = len(X); split = int(n*0.8)
    Xtr, Ytr = X[:split], Y[:split]; Xte, Yte = X[split:], Y[split:]

    # align forward returns for pnl proxy
    lab_ret = f"ret_fwd_{horizon_h}h"
    df = df.sort_values(["Symbol","Datetime"]).reset_index(drop=True)
    # crude approximation: use last len(Y) fwd rets
    all_fwd = df.groupby("Symbol")[lab_ret].apply(lambda s: s.iloc[seq_len:]).reset_index(drop=True).values
    all_fwd = all_fwd[-len(Y):] if len(all_fwd)>=len(Y) else np.zeros_like(Y)
    ftr, fte = all_fwd[:split], all_fwd[split:]

    # torch
    device = "cpu"
    import torch.optim as optim
    model = SmallGRU(n_features=X.shape[-1], hidden=48, dropout=0.1).to(device)
    opt = optim.Adam(model.parameters(), lr=1e-3)
    bce = torch.nn.BCELoss()
    train_loader = DataLoader(ArrDS(torch.tensor(Xtr), torch.tensor(Ytr)), batch_size=64, shuffle=True)
    test_loader  = DataLoader(ArrDS(torch.tensor(Xte), torch.tensor(Yte)), batch_size=128, shuffle=False)

    t0 = time.time(); ep_done=0
    for ep in range(epochs):
        model.train()
        for xb, yb in train_loader:
            xb, yb = xb.to(device), yb.to(device)
            opt.zero_grad(); out = model(xb); loss = bce(out, yb); loss.backward(); opt.step()
        ep_done += 1
        if time.time() - t0 > cap_min*60: break

    # eval
    model.eval(); preds=[]
    with torch.no_grad():
        for xb, yb in test_loader:
            xb = xb.to(device); out = model(xb).cpu().numpy(); preds.append(out)
    p = np.concatenate(preds) if preds else np.zeros((len(Yte),), np.float32)
    hr = _hit(p, Yte); br = _brier(p, Yte); pnl = _approx_pnl(p, Yte, np.array(fte[:len(p)]))

    ev = {
        "status":"ok","when_utc": dt.datetime.utcnow().isoformat()+"Z",
        "epochs": ep_done, "n_train": int(len(Ytr)), "n_test": int(len(Yte)),
        "hit_rate": round(hr,4), "brier": round(br,4), "pnl_proxy": pnl
    }
    os.makedirs(RPT, exist_ok=True); json.dump(ev, open(EVAL_PATH,"w"), indent=2)

    # keep history
    hist = []
    if os.path.exists(EVAL_HIST):
        try: hist = json.load(open(EVAL_HIST))
        except Exception: hist = []
    hist.append(ev); json.dump(hist[-60:], open(EVAL_HIST,"w"), indent=2)

    # save checkpoint
    os.makedirs(MODELS_DIR, exist_ok=True)
    try: torch.save(model.state_dict(), CKPT_PATH)
    except Exception: pass

    # readiness gates
    ready = (ev["epochs"] >= thr["min_epochs"] and ev["hit_rate"] >= thr["hit_rate"] and ev["brier"] <= thr["brier_max"])
    meta = {"ready": bool(ready), "evaluated_at": ev["when_utc"], "thresholds": thr, "eval": ev}

    # update DL kill-switch with this eval
    dks.update_from_eval(ev)
    json.dump(meta, open(META_PATH,"w"), indent=2)
    return meta

def predict_topk_if_ready(top_k=5):
    # check kill-switch
    st = dks.status()
    if not st.get("active", True):
        return pd.DataFrame(), "dl_suspended"

    try: meta = json.load(open(META_PATH))
    except Exception: return pd.DataFrame(), "dl_not_ready"
    if not meta.get("ready", False): return pd.DataFrame(), "dl_not_ready"

    # score latest window per symbol
    p = os.path.join(DL_DIR,"features_hourly.parquet")
    if not os.path.exists(p): return pd.DataFrame(), "dl_no_data"
    df = pd.read_parquet(p)
    if df.empty: return pd.DataFrame(), "dl_no_data"
    df = df.sort_values(["Symbol","Datetime"]).reset_index(drop=True)

    # features
    base = ["ret_1","ret_5","ret_20","ema_diff","rng","vol_z20","gap_pct","vix_norm","gift_norm","news_sent_1h"]
    feats = [c for c in base if c in df.columns]
    while len(feats) < 8: feats.append("ret_1")
    feats = feats[:8]

    import torch
    from dl_models.temporal import SmallGRU
    device = "cpu"
    model = SmallGRU(n_features=len(feats), hidden=48, dropout=0.0).to(device)
    try: model.load_state_dict(torch.load(CKPT_PATH, map_location=device))
    except Exception: return pd.DataFrame(), "dl_ckpt_missing"
    model.eval()

    rows = []
    with torch.no_grad():
        for s, g in df.groupby("Symbol"):
            g = g.reset_index(drop=True)
            if len(g) < CONFIG["dl"]["seq_len"] + CONFIG["dl"]["horizon_h"] + 1: continue
            X = g[feats].astype(np.float32).values
            xt = torch.tensor(X[-CONFIG["dl"]["seq_len"]:, :][None, ...]).to(device)
            p = float(model(xt).cpu().numpy().ravel()[0])
            close = float(g.iloc[-1]["Close"])
            entry, sl, tgt = close, close*0.99, close*1.01
            rows.append({"Symbol": s, "Entry": entry, "SL": sl, "Target": tgt, "proba": p, "Reason": "DL-GRU"})

    if not rows: return pd.DataFrame(), "dl_scored_none"
    import pandas as pd
    dfp = pd.DataFrame(rows).sort_values("proba", ascending=False).head(top_k)
    return dfp, "dl_ready"
