# -*- coding: utf-8 -*-
from __future__ import annotations
import sys, json
from config import CONFIG

def run():
    out = {"ok": True, "config_paths": CONFIG.get("paths",{})}
    try:
        import feature_store, matrix, pipeline_ai
        out["imports"] = True
        ff = feature_store.get_feature_frame(CONFIG.get("universe", []))
        X, cols, meta, stitched = matrix.build_matrix(ff)
        out["shape"] = [len(ff), len(cols)]
    except Exception as e:
        out["ok"] = False
        out["error"] = repr(e)
    print(json.dumps(out, indent=2))
