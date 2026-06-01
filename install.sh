#!/bin/bash
# install.sh — run this on a fresh Pi to set up the recorder
# Usage: sudo bash install.sh

set -euo pipefail

echo "==> Installing system dependencies"
apt-get update -qq
apt-get install -y ffmpeg python3-pip python3-venv

echo "==> Creating service user directories"
mkdir -p /etc/ot-agent
mkdir -p /var/lib/ot-recorder

# Only create env file if it doesn't exist yet
if [ ! -f /etc/ot-agent/env ]; then
    cp scripts/env.template /etc/ot-agent/env
    echo "==> Created /etc/ot-agent/env — EDIT THIS FILE before starting the service"
else
    echo "==> /etc/ot-agent/env already exists, skipping"
fi

chown -R pi:pi /var/lib/ot-recorder

echo "==> Installing Python package"
pip3 install --break-system-packages .

echo "==> Installing systemd service"
cp systemd/ot-recorder.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable ot-recorder.service

echo ""
echo "Done. Next steps:"
echo "  1. Edit /etc/ot-agent/env with your OT identity, S3 bucket, and HMS details"
echo "  2. sudo systemctl start ot-recorder"
echo "  3. sudo journalctl -u ot-recorder -f   # to watch logs"
