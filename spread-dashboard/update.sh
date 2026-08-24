#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
BRANCH="${UPDATE_BRANCH:-main}"
git fetch origin "$BRANCH"
git reset --hard "origin/$BRANCH"
source .venv/bin/activate
python -m pip install -r requirements.txt
exec python app.py
