# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
import json, traceback

from config import CONFIG

def _ok_path(p: Path): 
    return p.exists() and p.stat().st_size > 0

def run():
    out = {"ok": True, "did_train": [], "skipped": [], "reasons": {}}
    try:
        import feature_store, matrix

        # Load features
        uni = CONFIG.get("universe", [])
        ff = feature_store.get_feature_frame(uni)
        X, cols, meta, stitched = matrix.build_matrix(ff)

        n_rows = len(stitched)
        if n_rows < CONFIG.get("min_samples_for_heavy", 2000):
            out["ok"] = False
            out["reasons"]["data"] = f"Too few rows for heavy engines: {n_rows}"
            return out

        models_dir = Path("models")
        models_dir.mkdir(exist_ok=True)

        # ---- Boosters -------------------------------------------------------
        if CONFIG["engines"].get("boosters",{}).get("enabled", False):
            try:
                from model_robust import train_boosters  # should save under models/boost_*.pkl
                trained = train_boosters(X, stitched["y"].values, feature_cols=cols)
                out["did_train"].append({"boosters": trained})
            except Exception as e:
                out["ok"] = False
                out["reasons"]["boosters"] = repr(e)

        # ---- Deep Learning (FT / TCN / TST) --------------------------------
        if CONFIG["engines"].get("dl",{}).get("enabled", False):
            try:
                from dl_models.trainers import train_ft, train_tcn, train_tst
            except Exception as e:
                out["ok"] = False
                out["reasons"]["dl_import"] = repr(e)
                return out

            if CONFIG["engines"]["dl"].get("ft",{}).get("enabled", False):
                try:
                    p = models_dir/"dl_ft.pt"
                    if not _ok_path(p):
                        train_ft(stitched, cols, save_path=p)
                        out["did_train"].append("dl_ft")
                    else:
                        out["skipped"].append("dl_ft (exists)")
                except Exception as e:
                    out["ok"] = False
                    out["reasons"]["dl_ft"] = repr(e)

            if CONFIG["engines"]["dl"].get("tcn",{}).get("enabled", False):
                try:
                    p = models_dir/"dl_tcn.pt"
                    if not _ok_path(p):
                        train_tcn(stitched, cols, save_path=p)
                        out["did_train"].append("dl_tcn")
                    else:
                        out["skipped"].append("dl_tcn (exists)")
                except Exception as e:
                    out["ok"] = False
                    out["reasons"]["dl_tcn"] = repr(e)

            if CONFIG["engines"]["dl"].get("tst",{}).get("enabled", False):
                try:
                    p = models_dir/"dl_tst.pt"
                    if not _ok_path(p):
                        train_tst(stitched, cols, save_path=p)
                        out["did_train"].append("dl_tst")
                    else:
                        out["skipped"].append("dl_tst (exists)")
                except Exception as e:
                    out["ok"] = False
                    out["reasons"]["dl_tst"] = repr(e)

        # ---- Calibration ----------------------------------------------------
        if CONFIG["engines"].get("calibration",{}).get("enabled", False):
            try:
                from calibration import fit_calibrators  # saves under models/calibration/
                fit_calibrators(stitched, feature_cols=cols)
                out["did_train"].append("calibration")
            except Exception as e:
                out["ok"] = False
                out["reasons"]["calibration"] = repr(e)

        # ---- Stacker --------------------------------------------------------
        if CONFIG["engines"].get("stacker",{}).get("enabled", False):
            try:
                from stacker import fit_stacker  # saves models/stacker.pkl
                fit_stacker(stitched)
                out["did_train"].append("stacker")
            except Exception as e:
                out["ok"] = False
                out["reasons"]["stacker"] = repr(e)

    except Exception as e:
        out["ok"] = False
        out["reasons"]["bootstrap"] = repr(e)
        traceback.print_exc()

    Path("reports/debug").mkdir(parents=True, exist_ok=True)
    (Path("reports/debug")/"bootstrap_heavy.json").write_text(json.dumps(out, indent=2))
    print(json.dumps(out, indent=2))
    return out

if __name__ == "__main__":
    run()
