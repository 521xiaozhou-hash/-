#!/usr/bin/env bash
set -euo pipefail

REPO="https://github.com/521xiaozhou-hash/-.git"
BASE="/opt/spread-dashboard"
APP="$BASE/spread-dashboard"

apt-get update
apt-get install -y git python3 python3-venv python3-pip curl openssl

if [ -d "$BASE/.git" ]; then
  git -C "$BASE" fetch origin main
  git -C "$BASE" reset --hard origin/main
else
  mkdir -p "$(dirname "$BASE")"
  git clone --branch main "$REPO" "$BASE"
fi

cd "$APP"
bash install.sh

# Keep local configuration across OTA code updates.
if ! grep -q '^UPDATE_TOKEN=' .env 2>/dev/null; then
  TOKEN=$(openssl rand -hex 24)
  printf '\nUPDATE_TOKEN=%s\n' "$TOKEN" >> .env
fi
if ! grep -q '^BSTOCK_TICKERS=' .env 2>/dev/null; then
  printf 'BSTOCK_TICKERS=AAPL,TSLA,NVDA,MSTR\n' >> .env
fi

cat > /etc/systemd/system/spread-dashboard.service <<'EOF'
[Unit]
Description=Spread Dashboard + OTA Updater
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/spread-dashboard/spread-dashboard
ExecStart=/opt/spread-dashboard/spread-dashboard/.venv/bin/python /opt/spread-dashboard/spread-dashboard/supervisor.py
Restart=always
RestartSec=3
EnvironmentFile=-/opt/spread-dashboard/spread-dashboard/.env

[Install]
WantedBy=multi-user.target
EOF

# Remove the old two-process layout; supervisor now owns the web process.
systemctl disable --now spread-dashboard-updater.service 2>/dev/null || true
systemctl daemon-reload
systemctl enable --now spread-dashboard.service

TOKEN=$(grep '^UPDATE_TOKEN=' .env | cut -d= -f2-)
IP=$(hostname -I | awk '{print $1}')
echo
echo "=========================================="
echo "安装/升级完成"
echo "Dashboard: http://$IP:8080"
echo "程序目录: $BASE"
echo "自动 OTA 更新: 已启用"
echo "手动程序更新密钥: $TOKEN"
echo "请保存这个密钥，不要公开。"
echo "=========================================="
