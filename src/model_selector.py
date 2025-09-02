
def robust_ready(): 
    return False
def train_light():
    from .model_swing import train_model
    m, feats, data = train_model()
    return m, feats, data
def predict_light(m, feats):
    import pandas as pd
    # Placeholder predictions table
    return pd.DataFrame([{"Symbol":"RELIANCE","proba":0.6,"Entry":2500.0,"SL":2450.0,"Target":2550.0, "Reason":"stub"}])
def train_robust_model():
    from .model_robust import train_robust_model as tr
    return tr()
def predict_today_robust(m, feats):
    import pandas as pd
    return pd.DataFrame([{"Symbol":"HDFCBANK","proba":0.58,"Entry":1550.0,"SL":1519.0,"Target":1581.0,"Reason":"stub"}])
def choose_and_predict(top_k=5):
    preds = predict_light(object(), ["Close"])
    return preds.head(top_k), "light"
def choose_and_predict_full():
    return predict_light(object(), ["Close"]), "light"
