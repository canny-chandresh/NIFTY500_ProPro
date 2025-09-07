"""
Tiny experiment tracker (JSON).
"""

import json, datetime
from pathlib import Path
EXP_DIR = Path("experiments")
EXP_DIR.mkdir(exist_ok=True)

def log_experiment(params: dict, metrics: dict, name="run"):
    run_id = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    data = {"params":params,"metrics":metrics,"time":run_id}
    (EXP_DIR/f"{name}_{run_id}.json").write_text(json.dumps(data,indent=2))
    return run_id
