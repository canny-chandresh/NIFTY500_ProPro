# src/kill_switch.py
import os, pandas as pd, datetime as dt
from config import CONFIG, DL

STATE_FILE = "datalake/killswitch_state.csv"

def _read_state():
    if os.path.exists(STATE_FILE):
        try: return pd.read_csv(STATE_FILE)
        except Exception: pass
    return pd.DataFrame(columns=["date","status"])

def _write_state(status:str):
    df = _read_state()
    df = pd.concat([df, pd.DataFrame([{"date": dt.date.today(), "status": status}])], ignore_index=True)
    df.to_csv(STATE_FILE, index=False)

def _day_winrate(fp):
    if not os.path.exists(fp): return None
    try:
        df = pd.read_csv(fp)
        if df.empty: return None
        d = pd.to_datetime(df["date"]).dt.date
        g = df.groupby(d)["target_hit"].mean()
        return g.reindex(sorted(g.index))
    except Exception:
        return None

def evaluate_and_update():
    if not CONFIG["features"]["killswitch_v1"]:
        return {"status":"ACTIVE"}

    wr = _day_winrate(DL("paper_fills"))
    if wr is None or len(wr)==0:
        return {"status":"ACTIVE"}

    floor = CONFIG["killswitch"]["winrate_floor"]
    floor_days = CONFIG["killswitch"]["floor_days"]
    recent = wr.tail(floor_days).mean() if len(wr)>=floor_days else wr.mean()
    status = "ACTIVE"
    if recent < floor and len(wr)>=floor_days:
        status = "SUSPENDED"
    else:
        rec = CONFIG["killswitch"]["recovery_floor"]
        rdays = CONFIG["killswitch"]["recovery_days"]
        if len(wr)>=rdays and wr.tail(rdays).mean() >= rec:
            status = "ACTIVE"
    _write_state(status)
    return {"status": status}
