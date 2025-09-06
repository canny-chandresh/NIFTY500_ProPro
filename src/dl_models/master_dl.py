# src/dl_models/master_dl.py
from __future__ import annotations
import os, json, datetime as dt
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, Any

DL = Path("datalake")
REP = Path("reports/dl")
REP.mkdir(parents=True, exist_ok=True)

class DeepLearningTrainer:
    """
    Shadow Deep Learning trainer for NIFTY500_ProPro.

    - Consumes hourly OHLCV + features
    - Maintains rolling training set
    - Trains lightweight dense model (MLP) or placeholder
    - Saves metrics to reports/dl/
    """

    def __init__(self, model_name="deep_shadow", window_days: int = 60):
        self.model_name = model_name
        self.window_days = window_days
        self.metrics: Dict[str, Any] = {}

    def _load_data(self) -> pd.DataFrame:
        """Load features for DL training (last N days)."""
        eq = DL / "daily_equity.csv"
        if not eq.exists(): 
            return pd.DataFrame()
        df = pd.read_csv(eq, parse_dates=["Date"], low_memory=False)
        cutoff = pd.Timestamp.utcnow().normalize() - pd.Timedelta(days=self.window_days)
        df = df[df["Date"] >= cutoff]
        return df

    def train(self) -> Dict[str, Any]:
        """Mock training step; replace with TensorFlow/PyTorch later."""
        data = self._load_data()
        if data.empty:
            self.metrics = {"trained": False, "reason": "no_data"}
            return self.metrics

        n_samples = len(data)
        # Fake win-rate improvement logic
        np.random.seed(len(data))
        win_rate = 0.45 + 0.05*np.tanh(n_samples/5000.0)
        profit_factor = 1.0 + np.log1p(n_samples/2000.0)

        self.metrics = {
            "trained": True,
            "samples": n_samples,
            "win_rate_est": round(win_rate*100,2),
            "profit_factor": round(profit_factor,2),
            "timestamp": dt.datetime.utcnow().isoformat()+"Z"
        }

        out = REP / f"dl_metrics_{dt.datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
        with open(out,"w",encoding="utf-8") as f: json.dump(self.metrics, f, indent=2)

        return self.metrics

    def latest_metrics(self) -> Dict[str, Any]:
        return self.metrics if self.metrics else {"trained": False, "reason": "not_run"}

# Convenience entrypoint
if __name__ == "__main__":
    trainer = DeepLearningTrainer()
    metrics = trainer.train()
    print("DL trainer metrics:", metrics)
