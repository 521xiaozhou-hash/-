#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
PYTHON_BIN="${PYTHON_BIN:-python3}"
$PYTHON_BIN -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
[ -f .env ] || cp .env.example .env
cat <<'EOF'

安装完成。
启动：
  cd spread-dashboard
  source .venv/bin/activate
  python app.py

访问：
  http://服务器IP:8080

生产环境建议使用 nginx/reverse proxy，并按需调整 .env 的 PORT、REFRESH_SECONDS、BSTOCK_SYMBOLS。
EOF
