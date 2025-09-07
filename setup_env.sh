#!/usr/bin/env bash
set -euo pipefail

echo "== 🧰 Environment bootstrap =="
PYVER="${1:-3.11}"

# 1) Show runner basics
echo "-> Python target: ${PYVER}"
python3 --version || true
pip3 --version || true
uname -a || true

# 2) Upgrade pip/wheels first (avoid manylinux issues)
python3 -m pip install --upgrade pip wheel setuptools

# 3) Install project requirements (if present)
if [[ -f "requirements.txt" ]]; then
  echo "-> Installing requirements.txt"
  python3 -m pip install -r requirements.txt
else
  echo "⚠️  requirements.txt not found; skipping."
fi

# 4) Optional speed-ups / CUDA skip on CI
# (Torch CPU wheels install by default; no CUDA required on GitHub runners)
python3 - <<'PY'
import sys, pkgutil
print("== ✅ Installed key packages ==")
for mod in ["pandas","numpy","sklearn","xgboost","torch","yfinance","networkx","requests","pyarrow","shap","transformers"]:
    ok = pkgutil.find_loader(mod) is not None
    print(f"{mod:12s} : {'OK' if ok else 'MISS'}")
PY

echo "== ✅ Env ready =="
