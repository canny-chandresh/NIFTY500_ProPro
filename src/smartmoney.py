
import pandas as pd

def smart_money_today(symbols):
    # Placeholder: neutral SMS 0.6 for all
    if symbols is None: 
        return pd.DataFrame(columns=["Symbol","sms_score","sms_reasons"])
    return pd.DataFrame({"Symbol": symbols, "sms_score": [0.6]*len(symbols), "sms_reasons": ["neutral"]*len(symbols)})
