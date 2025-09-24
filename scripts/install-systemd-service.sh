#!/usr/bin/env bash
# Install a systemd service for MESH-AI so it starts at boot on headless systems.
# Usage:
#   sudo ./scripts/install-systemd-service.sh [/absolute/path/to/mesh-ai]
# Defaults to the folder containing this script.
set -euo pipefail

if [ "${EUID}" -ne 0 ]; then
  echo "Please run as root: sudo $0"
  exit 1
fi

# Resolve repo dir
if [ $# -ge 1 ]; then
  # Resolve to absolute path
  REPO_DIR="$(readlink -f "$1")"
else
  REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
fi

APP_USER="$(logname 2>/dev/null || whoami)"
APP_GROUP="$(id -gn "$APP_USER")"
SERVICE_NAME="mesh-ai"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"

START_SCRIPT="$REPO_DIR/start_mesh_ai.sh"
if [ ! -x "$START_SCRIPT" ]; then
  echo "Making start script executable: $START_SCRIPT"
  chmod +x "$START_SCRIPT"
fi

cat > "$SERVICE_FILE" <<EOF
[Unit]
Description=MESH-AI Service
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$APP_USER
Group=$APP_GROUP
WorkingDirectory=$REPO_DIR
Environment=PYTHONUNBUFFERED=1
# Optionally set port here, or set in config.json (web_port/flask_port/port)
# Environment=MESH_AI_PORT=5000
ExecStart=$START_SCRIPT
Environment=FOREGROUND=1
Environment=NO_BROWSER=1
Restart=on-failure
RestartSec=10

# Give it a little more time to stop cleanly
TimeoutStopSec=20

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable "$SERVICE_NAME"

echo "Installed systemd service at $SERVICE_FILE"
echo "You can start it now with: sudo systemctl start $SERVICE_NAME"
echo "And check status with:     sudo systemctl status $SERVICE_NAME"
