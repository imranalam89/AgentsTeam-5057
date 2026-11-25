# AgentsTeam-5057 — Unified 3-Asset Multi-Broker Live Engine (Port 5057)

Production 24/7 trading engine and full-control HTML command desk for **Gold (`XAUTUSD`)**, **Bitcoin (`BTCUSD`)**, and **Ethereum (`ETHUSD`)** running strictly on **Port `5057`** with **zero third-party Python dependencies** (100% Python 3 Standard Library).

---

## Core Architecture & Fixes Included

1. **`24,000`-Bar 1m RAM Primer (`EMA(7200)` Convergence):** Primes `24,000` official 1-minute candles (~16.7 days) on startup so macro `EMA(7200)` and `EMA(1440)` match institutional backtests with `< 15 ms` context evaluation.
2. **`1m` Mean-L2 Closed-Candle Entry + `1-Second` Spread-Guarded Exit:**
   - **Entries:** Averages 60 seconds of live L2 order-book depth per minute (`ratio`, `ratio_10`, `imbalance`, `spread`) — with `50/50` native CLV blend on Gold — eliminating single-second `:02s` L2 flicker.
   - **Exits:** Evaluates Stop-Loss, Maker Partial TPs, Breakeven (`+1.05R`), and Continuous S/R-Aware Ratchet Trailing SL every **1 second** (guarded by `spread <= max_spread` to block thin-book spread spikes), while candle/order-flow reversal cuts (`SMART_STALL_LOCK`, `PRE_SL_CUT`, `FAST_REVERSAL_SAVE`, `FLOW_REVERSAL_CUT`) evaluate strictly on `1m` bar close.
3. **Full Co-Firing Router Evaluation & Per-Specialist Cooldowns:**
   - Evaluates all co-firing candidates in each direction so alphabetical sorting never masks Day × Session Router champions (e.g. `L2_COIL_MACRO_SUPPLY_TRAP`).
   - Enforces per-`(specialist, timeframe)` cooldowns (`90m` BTC `1m`, `60m` BTC `5m`, `45m` Gold `1m/15m`, etc.) plus per-pair `1m` timeframe toggles in the Admin UI.
4. **Delta Maker-First Entry Chaser & India `18%` GST Fee Accounting:**
   - Supports `MAKER_CHASER` (`post_only` best bid/ask for `2.4s` → automatic Market fallback so zero trades are missed) or immediate `MARKET` mode per Delta account, with `18%` GST-inclusive fee tracking synced to `/v2/fills`.
5. **Strict Public vs. Admin Account Privacy:**
   - **Public View (Logged Out):** Displays **ONLY** your **Default `DELTA` Account** (calculations, KPIs, equity curve, open positions, calendar, and trade ledger).
   - **Admin View (After Password Login Only):** Add, configure, test, and inspect **full dedicated data** (`View Full Data` / Account Switcher) for any secondary account:
     - **Delta Exchange India** (`DELTA`)
     - **Tradovate via PickMyTrade V2 Webhook** (`PICKMYTRADE` — paste alert JSON, 1-click import, live payload preview & test ping)
     - **Universal Outbound Webhook** (`WEBHOOK` — Standard JSON, TradersPost, PineConnector, or Custom Template)
     - **MetaTrader 5 REST / EA Poll Bridge** (`MT5_API`)
     - **Direct Tradovate REST API** (`TRADOVATE_API`)
     - **Inbound Signal Webhook Generator & Live Manual Signal Tester** (`/api/webhook/inbound`)

---

## Strict VPS Port Isolation

- Dedicated Folder: `/var/www/agents_team_5057`
- Dedicated Systemd Service: `agents_team_5057.service`
- Dedicated TCP Port: **`5057` ONLY**
- **Ports `5050`, `5055`, and `5056` remain 100% untouched.**

---

## 1-Minute Hostinger VPS Deployment

SSH into your Hostinger VPS and run:

```bash
mkdir -p /var/www && cd /var/www
git clone https://github.com/imranalam89/AgentsTeam-5057.git agents_team_5057
cd /var/www/agents_team_5057
cp .env.example .env
nano .env   # Set a strong ADMIN_PASSWORD (and optional DELTA_API_KEY / SECRET, or add via Web UI)
chmod +x setup_hostinger_5057.sh
./setup_hostinger_5057.sh
```

Then open:
- **Live Journal & Admin Command Center:** `http://YOUR_VPS_IP:5057/`
- **16-Agent Strategy Architecture:** `http://YOUR_VPS_IP:5057/agents`

### Useful Service Commands

```bash
# Check service status
systemctl status agents_team_5057

# Tail live 1s L2 & multi-broker execution logs
journalctl -u agents_team_5057 -f

# Pull future updates cleanly (never overwrites .env, accounts, or trade journal)
cd /var/www/agents_team_5057 && git pull && systemctl restart agents_team_5057
```
