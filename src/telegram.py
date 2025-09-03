# src/telegram.py
import os, requests, pandas as pd
from config import DL

def send_message(text: str):
    token = os.environ.get("TG_BOT_TOKEN")
    chat  = os.environ.get("TG_CHAT_ID")
    if not token or not chat: 
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        requests.post(url, data={"chat_id": chat, "text": text, "parse_mode":"Markdown"} , timeout=10)
        return True
    except Exception:
        return False

def status_payload():
    wr = "NA"
    pf = DL("paper_fills")
    if os.path.exists(pf):
        try:
            df = pd.read_csv(pf)
            df["date"] = pd.to_datetime(df["date"]).dt.date
            g = df.groupby("date")["target_hit"].mean().tail(7)
            wr = f"{g.mean():.2%}" if len(g)>0 else "NA"
        except Exception:
            pass
    ks = "ACTIVE"
    ksfp = "datalake/killswitch_state.csv"
    if os.path.exists(ksfp):
        try:
            ksdf = pd.read_csv(ksfp)
            if not ksdf.empty:
                ks = ksdf.iloc[-1]["status"]
        except Exception:
            pass
    return f"Status: winrate(7d)={wr}, kill-switch={ks}"
