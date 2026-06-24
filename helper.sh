#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/home/pi/alpha_ws_mqtt"
SERVICE_NAME="alpha-ws-mqtt.service"
SERVICE_DST="/etc/systemd/system/${SERVICE_NAME}"

if [[ $EUID -ne 0 ]]; then
  echo "Run as root:"
  echo "  sudo ./helper.sh"
  exit 1
fi

cd "$APP_DIR"

echo "==> Checking files"

for f in "alpha_ws_mqtt.py" "alpha_ws_mqtt.yaml" "${SERVICE_NAME}"; do
  if [[ ! -f "$f" ]]; then
    echo "Missing file: ${APP_DIR}/$f"
    exit 1
  fi
done

echo "==> Installing system Python packages"
apt update
apt install -y python3 python3-pip python3-yaml python3-paho-mqtt python3-websocket

echo "==> Checking Python imports"
if ! sudo -u pi /usr/bin/python3 - <<'PY'
import yaml
import paho.mqtt.client
import websocket
print("imports ok")
PY
then
  echo "==> Apt packages incomplete, installing with pip into system Python"
  /usr/bin/python3 -m pip install --break-system-packages --upgrade PyYAML paho-mqtt websocket-client
fi

echo "==> Installing systemd service"
cp "${SERVICE_NAME}" "$SERVICE_DST"
chmod 644 "$SERVICE_DST"

echo "==> Ensuring ownership"
chown -R pi:pi "$APP_DIR"

echo "==> Making script executable"
chmod 755 "${APP_DIR}/alpha_ws_mqtt.py"

echo "==> Reloading systemd"
systemctl daemon-reload

echo "==> Enabling service"
systemctl enable "$SERVICE_NAME"

echo "==> Restarting service"
systemctl restart "$SERVICE_NAME"

echo "==> Status"
systemctl --no-pager --full status "$SERVICE_NAME" || true

echo
echo "Logs:"
echo "  journalctl -u ${SERVICE_NAME} -f"
echo
echo "Restart:"
echo "  sudo systemctl restart ${SERVICE_NAME}"
echo
echo "Stop:"
echo "  sudo systemctl stop ${SERVICE_NAME}"