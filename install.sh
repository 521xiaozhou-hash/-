#!/usr/bin/env bash
set -euo pipefail

REPO="https://github.com/521xiaozhou-hash/-.git"
BASE="/opt/spread-dashboard"

apt-get update
apt-get install -y git python3 python3-venv python3-pip curl

if [ -d "$BASE/.git" ]; then
  git -C "$BASE" fetch origin main
  git -C "$BASE" reset --hard origin/main
else
  mkdir -p "$(dirname "$BASE")"
  git clone --branch main "$REPO" "$BASE"
fi

cd "$BASE/spread-dashboard"
bash install.sh

cat > /etc/systemd/system/spread-dashboard.service <<'EOF'
[Unit]
Description=Spread Dashboard
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/spread-dashboard/spread-dashboard
ExecStart=/opt/spread-dashboard/spread-dashboard/.venv/bin/python /opt/spread-dashboard/spread-dashboard/app.py
Restart=always
RestartSec=3
EnvironmentFile=-/opt/spread-dashboard/spread-dashboard/.env

[Install]
WantedBy=multi-user.target
EOF

cat > /etc/systemd/system/spread-dashboard-updater.service <<'EOF'
[Unit]
Description=Spread Dashboard Auto Updater
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/spread-dashboard/spread-dashboard
ExecStart=/opt/spread-dashboard/spread-dashboard/supervisor.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

chmod +x /opt/spread-dashboard/spread-dashboard/supervisor.py
systemctl daemon-reload
systemctl enable --now spread-dashboard.service
systemctl enable --now spread-dashboard-updater.service

echo
 echo "=========================================="
echo "安装完成"
echo "Dashboard: http://$(hostname -I | awk '{print $1}'):8080"
echo "程序目录: $BASE"
echo "自动更新: 已启用"
echo "=========================================="
