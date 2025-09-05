from __future__ import annotations
import pandas as pd

def choose_and_predict_full(top_k: int = 5):
    # 1) Try DL if ready and not suspended
    try:
        import dl_runner
        dl_df, tag = dl_runner.predict_topk_if_ready(top_k=top_k)
        if dl_df is not None and not dl_df.empty and tag == "dl_ready":
            return dl_df, "dl"
    except Exception:
        pass

    # 2) Fallback: your Light/Robust
    try:
        from model_swing import predict_today
        preds = predict_today(top_k=top_k)
        return preds, "light"
    except Exception:
        return pd.DataFrame(), "light"
