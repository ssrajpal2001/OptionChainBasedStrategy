#!/usr/bin/env bash
# setup_https.sh — install Caddy as an HTTPS reverse proxy in front of the dashboard (:5000).
# Gives the app a real Let's Encrypt cert on a raw EC2 IP via sslip.io (no domain needed), so
# brokers (Upstox/Zerodha/Fyers) accept the OAuth redirect URI.
#
# After this runs, the app is reachable at  https://<dashed-ip>.sslip.io
# Then in the UI: Admin Workspace -> Data Feeder -> Global Redirect Base = that https URL,
# and register https://<dashed-ip>.sslip.io/callback/{zerodha,upstox,fyers} in each broker.
#
# Requirements: EC2 security group must allow inbound 80 AND 443. Run with sudo privileges.
# Usage:  bash scripts/setup_https.sh                 # auto-detects public IP
#         bash scripts/setup_https.sh 13.200.171.160  # or pass the IP explicitly
set -e

IP="${1:-$(curl -s https://checkip.amazonaws.com || curl -s ifconfig.me)}"
IP="$(echo "$IP" | tr -d '[:space:]')"
HOST="$(echo "$IP" | tr '.' '-').sslip.io"
echo "== setup_https: IP=$IP  ->  host=$HOST =="

# 1. Install the Caddy static binary (works on any Linux; no distro repo needed)
if ! command -v caddy >/dev/null 2>&1; then
    echo "-- downloading Caddy binary"
    curl -sL -o /tmp/caddy "https://caddyserver.com/api/download?os=linux&arch=amd64"
    sudo mv /tmp/caddy /usr/bin/caddy
    sudo chmod +x /usr/bin/caddy
fi
caddy version

# 2. Dedicated system user + config dir
sudo groupadd --system caddy 2>/dev/null || true
sudo useradd --system --gid caddy --create-home --home-dir /var/lib/caddy \
    --shell /usr/sbin/nologin caddy 2>/dev/null || true
sudo mkdir -p /etc/caddy

# 3. Caddyfile — auto-TLS for the sslip.io host, reverse-proxy to the dashboard on :5000
sudo tee /etc/caddy/Caddyfile > /dev/null <<EOF
$HOST {
    reverse_proxy localhost:5000
}
EOF

# 4. systemd service (CAP_NET_BIND_SERVICE lets the non-root user bind 80/443)
sudo tee /etc/systemd/system/caddy.service > /dev/null <<'EOF'
[Unit]
Description=Caddy HTTPS reverse proxy
After=network.target

[Service]
User=caddy
Group=caddy
ExecStart=/usr/bin/caddy run --environ --config /etc/caddy/Caddyfile
ExecReload=/usr/bin/caddy reload --config /etc/caddy/Caddyfile
Restart=on-failure
AmbientCapabilities=CAP_NET_BIND_SERVICE

[Install]
WantedBy=multi-user.target
EOF

# 5. Start + enable on boot
sudo systemctl daemon-reload
sudo systemctl enable --now caddy
sleep 3
sudo systemctl status caddy --no-pager | head -12

echo ""
echo "== done. Your app should now be at: https://$HOST =="
echo "   Verify (cert may take ~20s on first run):  curl -I https://$HOST"
echo "   1) UI -> Admin Workspace -> Data Feeder -> Global Redirect Base = https://$HOST"
echo "   2) Register in each broker:  https://$HOST/callback/zerodha  (and /upstox /fyers)"
