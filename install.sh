#!/bin/bash
# install.sh — run this on a fresh Pi/Ubuntu host to set up the recorder
# Usage: sudo bash install.sh
#        OT_RECORDER_USER=ubuntu sudo bash install.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ -n "${OT_RECORDER_USER:-}" ]; then
    SERVICE_USER="$OT_RECORDER_USER"
elif [ -n "${SUDO_USER:-}" ] && [ "$SUDO_USER" != "root" ]; then
    SERVICE_USER="$SUDO_USER"
else
    SERVICE_USER="pi"
fi

if ! id "$SERVICE_USER" &>/dev/null; then
    echo "ERROR: user '$SERVICE_USER' does not exist." >&2
    echo "Set OT_RECORDER_USER to a valid account, e.g.:" >&2
    echo "  OT_RECORDER_USER=ubuntu sudo bash install.sh" >&2
    exit 1
fi

SERVICE_GROUP="$(id -gn "$SERVICE_USER")"

echo "==> Installing system dependencies"
apt-get update -qq
apt-get install -y ffmpeg python3-pip python3-venv

echo "==> Creating service user directories"
mkdir -p /etc/ot-agent
mkdir -p /var/lib/ot-recorder

# Only create env file if it doesn't exist yet
if [ ! -f /etc/ot-agent/env ]; then
    cp "$SCRIPT_DIR/env.template" /etc/ot-agent/env
    echo "==> Created /etc/ot-agent/env — EDIT THIS FILE before starting the service"
else
    echo "==> /etc/ot-agent/env already exists, skipping"
fi

chown -R "$SERVICE_USER:$SERVICE_GROUP" /var/lib/ot-recorder

echo "==> Installing Python package"
# Don't pip-upgrade wheel on Debian/Ubuntu — apt installs it without pip RECORD metadata.
pip3 install --break-system-packages --upgrade --ignore-installed pip setuptools
pip3 install --break-system-packages "$SCRIPT_DIR"

echo "==> Installing systemd service (User=$SERVICE_USER, Group=$SERVICE_GROUP)"
sed -e "s/^User=.*/User=$SERVICE_USER/" \
    -e "s/^Group=.*/Group=$SERVICE_GROUP/" \
    "$SCRIPT_DIR/ot-recorder.service" > /etc/systemd/system/ot-recorder.service
systemctl daemon-reload
systemctl enable ot-recorder.service

echo ""
echo "Done. Next steps:"
echo "  1. Edit /etc/ot-agent/env with OT identity, S3, and ThingsBoard device token"
echo "  2. sudo systemctl start ot-recorder"
echo "  3. sudo journalctl -u ot-recorder -f   # to watch logs"
