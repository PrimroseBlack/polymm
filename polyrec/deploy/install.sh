#!/usr/bin/env bash
# Install polyrec on a Debian/Ubuntu VPS. Run as root from a checkout of this repository:
#   sudo bash deploy/install.sh
# Re-running it upgrades the installed code and keeps /etc/polyrec.toml and the data.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
APP=/opt/polyrec
DATA=/var/lib/polyrec

python3 -c 'import sys; sys.exit(sys.version_info < (3, 11))' \
  || { echo "Python 3.11+ is required (Ubuntu 24.04 or Debian 12 have it)"; exit 1; }
if ! python3 -c 'import venv, ensurepip' 2>/dev/null; then
  apt-get update && apt-get install -y python3-venv
fi

id polyrec >/dev/null 2>&1 || useradd --system --home-dir "$APP" --shell /usr/sbin/nologin polyrec
mkdir -p "$APP" "$DATA"
python3 -m venv "$APP/.venv"
"$APP/.venv/bin/pip" install --quiet --upgrade pip
"$APP/.venv/bin/pip" install --quiet --upgrade "$REPO[creds]"

if [ ! -f /etc/polyrec.toml ]; then
  cp "$REPO/polyrec.example.toml" /etc/polyrec.toml
  echo "wrote /etc/polyrec.toml (data in $DATA)"
fi
chown -R polyrec:polyrec "$APP" "$DATA"

cp "$REPO"/deploy/polyrec.service "$REPO"/deploy/polyrec-derive.service "$REPO"/deploy/polyrec-derive.timer \
  /etc/systemd/system/
systemctl daemon-reload

echo
echo "Installed. Next:"
echo "  sudo -u polyrec $APP/.venv/bin/polyrec -c /etc/polyrec.toml smoke     # check the feeds from this host"
echo "  (optional, for Chainlink prices: see 'Chainlink prices' in the README)"
echo "  systemctl enable --now polyrec.service polyrec-derive.timer          # start recording"
echo "  journalctl -u polyrec -f                                             # watch it"
