#!/bin/bash
# ==============================================================================
# 🏛️ AGENTS TEAM 5057 — MULTI-BROKER 3-ASSET LIVE ENGINE (GOLD + BTC + ETH)
# Target Host : Hostinger VPS (Dedicated Port 5057 ONLY)
# Isolation   : Ports 5050, 5055 & 5056 and their services are 100% UNTOUCHED
# Public View : Default DELTA Account Only (Tradovate/Webhook/MT5 Admin-Only)
# ==============================================================================

set -e

echo "=================================================================="
echo "  🏛️ AGENTS TEAM 5057 — ISOLATED HOSTINGER VPS DEPLOYMENT        "
echo "  Engine  : 1m Mean-L2 Closed-Candle Entry + 1s Spread-Guarded Exit"
echo "  Port    : 5057 ONLY (Ports 5050, 5055 & 5056 100% untouched)    "
echo "=================================================================="

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
echo "📍 Application Directory: $APP_DIR"

# 1. Verify Python 3 (Zero pip packages required — 100% Python 3 stdlib)
echo "📦 Step 1: Checking Python 3..."
if command -v python3 &>/dev/null; then
    echo "✅ Python 3 is installed: $(python3 --version)"
else
    echo "Installing Python 3..."
    dnf install -y python3 2>/dev/null || (apt-get update && apt-get install -y python3)
fi

# 2. Initialize .env if not present
echo "🔑 Step 2: Checking environment configuration..."
if [ ! -f "$APP_DIR/.env" ]; then
    if [ -f "$APP_DIR/.env.example" ]; then
        cp "$APP_DIR/.env.example" "$APP_DIR/.env"
        chmod 600 "$APP_DIR/.env"
        echo "✅ Created $APP_DIR/.env from .env.example (chmod 600)."
    fi
else
    chmod 600 "$APP_DIR/.env" 2>/dev/null || true
    echo "✅ Existing $APP_DIR/.env preserved."
fi

# 3. Run quick syntax & health check before starting systemd service
echo "🧪 Step 3: Running pure-stdlib verification check..."
python3 "$APP_DIR/healthcheck_5057.py"

# 4. Install Dedicated Systemd Service: agents_team_5057.service (Port 5057 ONLY)
echo "⚙️ Step 4: Installing dedicated systemd service (agents_team_5057.service)..."
cat << EOF > /etc/systemd/system/agents_team_5057.service
[Unit]
Description=AgentsTeam 5057 Multi-Broker 3-Asset (Gold + BTC + ETH) Live Engine (Port 5057)
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=$APP_DIR
ExecStart=/usr/bin/python3 $APP_DIR/hostinger_multibroker_live.py
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal
Environment=MULTIBROKER_PORT=5057
Environment=PORT=5057
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
EOF

# 5. Open Firewall Port 5057 ONLY (never touches 5050, 5055, or 5056)
echo "🛡️ Step 5: Allowing TCP Port 5057 in firewall..."
if command -v ufw &>/dev/null; then
    ufw allow 5057/tcp 2>/dev/null || true
elif command -v firewall-cmd &>/dev/null; then
    firewall-cmd --permanent --add-port=5057/tcp 2>/dev/null || true
    firewall-cmd --reload 2>/dev/null || true
fi

if [ -f /etc/csf/csf.conf ]; then
    if ! grep -q "5057" /etc/csf/csf.conf; then
        sed -i 's/TCP_IN = "/TCP_IN = "5057,/g' /etc/csf/csf.conf
        csf -r 2>/dev/null || true
    fi
fi

# 6. Enable & Start ONLY agents_team_5057.service
echo "🚀 Step 6: Starting agents_team_5057.service..."
systemctl daemon-reload
systemctl enable agents_team_5057.service
systemctl restart agents_team_5057.service

VPS_IP="$(hostname -I 2>/dev/null | awk '{print $1}')"
VPS_IP="${VPS_IP:-62.72.31.29}"
echo ""
echo "=================================================================="
echo "  ✅ AGENTS TEAM 5057 DEPLOYED & RUNNING 24/7 ON PORT 5057!       "
echo "=================================================================="
echo "  📊 Live Trading Journal : http://${VPS_IP}:5057/"
echo "  🧠 16-Agent Architecture: http://${VPS_IP}:5057/agents"
echo "  🤖 Service Status       : systemctl status agents_team_5057"
echo "  📋 Live Engine Logs     : journalctl -u agents_team_5057 -f"
echo "  🔒 Public Privacy       : Default DELTA Account Only"
echo "  🛡️ Ports 5050/5055/5056 : 100% UNTOUCHED"
echo "=================================================================="
