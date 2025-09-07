# src/weekly_diagnostic.py
from __future__ import annotations
from pathlib import Path
import pandas as pd, json, datetime as dt, os

DL = Path("datalake"); FEAT = DL / "features"
REP = Path("reports/weekly"); REP.mkdir(parents=True, exist_ok=True)

def run() -> dict:
    rows=[]
    for p in sorted(FEAT.glob("*_features.csv"))[:200]:
        try:
            df = pd.read_csv(p, nrows=5)  # header only for speed
            cols = [c for c in df.columns if c not in ("Date")]
            auto = len([c for c in cols if c.startswith("AUTO_")])
            rows.append({"file": p.name, "cols": len(cols), "auto": auto})
        except Exception:
            pass
    sm = pd.DataFrame(rows)
    out = {
        "when_utc": dt.datetime.utcnow().isoformat()+"Z",
        "files": int(len(sm)),
        "avg_cols": float(sm["cols"].mean()) if not sm.empty else 0.0,
        "avg_auto": float(sm["auto"].mean()) if not sm.empty else 0.0,
        "top10_by_cols": sm.sort_values("cols", ascending=False).head(10).to_dict(orient="records")
    }
    (REP / "feature_summary.json").write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(json.dumps(out, indent=2))
    return out

if __name__ == "__main__":
    run()
