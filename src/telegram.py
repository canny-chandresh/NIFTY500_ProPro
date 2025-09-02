
import os, requests

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

# Minimal /status responder placeholder (actual polling done in Actions run)
def poll_and_respond_status():
    return
