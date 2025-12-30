# Solana CEX Block Monitor Bot

A lightweight, real-time Solana blockchain monitor that detects outgoing SOL transfers from configured CEX wallets and sends Telegram alerts when transaction amounts fall within configured ranges.

## Features

- **Real-time block monitoring** — Processes new blocks as they appear (~1-2 sec latency)
- **Ultra-low RPC usage** — Only uses `getSlot()` and `getBlock()` calls (no archive/history queries)
- **Zero rate-limiting** — Works reliably on free RPC tiers
- **Range-based alerting** — Customize SOL amount ranges per CEX wallet
- **Telegram notifications** — Instant alerts with clickable Solscan links
- **Automatic RPC failover** — Switches to secondary RPC on failure
- **Production-ready** — Minimal logging, error handling, graceful shutdown

## Prerequisites

- Node.js 18+ with npm
- Telegram Bot Token and Chat ID
- Solana RPC endpoint (free tier works fine)

## Installation

```bash
git clone https://github.com/fele-scratch/CEX-WALLET-TRACKER-real.git
cd CEX-WALLET-TRACKER-real
npm install
```

## Configuration

Copy `.env.example` to `.env` and fill in your values:

```bash
cp .env.example .env
```

### `.env` Template

```dotenv
# ---- CEX Wallet 1 ----
CEX_1_LABEL=OKX
CEX_1_ADDRESS=is6MTRHEgyFLNTfYcuV4QBWLjrZBfmhVNYR6ccgr8KV
CEX_1_RANGE=17-21,50-100,200-300

# ---- CEX Wallet 2 ----
CEX_2_LABEL=KUCOIN
CEX_2_ADDRESS=BmFdpraQhkiDQE6SnfG5omcA1VwzqfXrwtNYBwWTymy6
CEX_2_RANGE=49-51,100-150

# RPC Endpoints
RPC_PRIMARY=https://mainnet.helius-rpc.com/?api-key=YOUR-API-KEY
RPC_SECONDARY=https://api.mainnet-beta.solana.com

# Block Monitoring (optional, defaults shown)
BLOCK_POLL_MIN_MS=400
BLOCK_POLL_MAX_MS=700

# Telegram
TELEGRAM_BOT_TOKEN=your-bot-token-here
TELEGRAM_CHAT_ID=your-chat-id-here
```

### Configuration Details

**CEX Wallets:**
- `CEX_n_LABEL` — Display name (e.g., OKX, KUCOIN)
- `CEX_n_ADDRESS` — Wallet public address to monitor
- `CEX_n_RANGE` — Comma-separated SOL ranges (e.g., `17-21,50-100` means alert on transfers of 17-21 SOL or 50-100 SOL)

**RPC:**
- `RPC_PRIMARY` — Primary RPC endpoint (recommended: Helius, QuickNode, or public endpoint)
- `RPC_SECONDARY` — Fallback RPC (optional)

**Telegram:**
- Get `TELEGRAM_BOT_TOKEN` from [@BotFather](https://t.me/botfather) on Telegram
- Get `TELEGRAM_CHAT_ID` by sending a message to your bot and checking `/getMe`

## Local Development

Build and run:

```bash
npm run build
npm start
```

Or run with TypeScript directly (requires ts-node):

```bash
npm run dev
```

Listener-based mode (WebSocket)
--------------------------------

A listener-based entry point is available that uses WebSocket slot updates for lower-latency, near-real-time processing.

Build and run the listener:

```bash
npm run build
npm run start-listener
```

Use `npm run dev-listener` to run the listener in TypeScript during development.

## Deployment on Render

### Step 1: Push to GitHub

```bash
git add .
git commit -m "Initial commit: Solana CEX Block Monitor"
git push origin main
```

### Step 2: Create Render Service

1. Go to [render.com](https://render.com)
2. Click **New +** → **Web Service**
3. Connect your GitHub repository
4. Fill in:
   - **Name:** `solana-cex-monitor` (or your choice)
   - **Environment:** `Node`
   - **Build Command:** `npm install && npm run build`
   - **Start Command:** `node dist/index.js`
   - **Plan:** Free tier is fine

### Step 3: Add Environment Variables

In Render dashboard:
1. Go to your service settings
2. Click **Environment**
3. Add all variables from `.env`:
   - `CEX_1_LABEL`, `CEX_1_ADDRESS`, `CEX_1_RANGE`
   - `CEX_2_LABEL`, `CEX_2_ADDRESS`, `CEX_2_RANGE`
   - `RPC_PRIMARY`, `RPC_SECONDARY`
   - `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`

### Step 4: Deploy

1. Click **Deploy**
2. Watch logs to confirm startup
3. Bot will run 24/7 on Render

## Project Structure

```
src/
  index.ts                 # Entry point
  loaders/
    rpc.ts               # RPC manager with failover
  services/
    blockMonitor.ts      # Real-time block monitoring
    blockParser.ts       # Transaction parsing
    ranges.ts            # Range matching logic
  utils/
    logger.ts            # Logging
    telegram.ts          # Telegram alerting
    sleep.ts             # Utility sleep function

dist/                     # Compiled JavaScript (generated)
.env.example              # Configuration template
```

## How It Works

1. **Listener-based block monitoring** — Subscribes to WebSocket slot updates and fetches blocks as they are rooted
2. **Fast Fetching** — Newly-rooted slots are fetched immediately and queued for processing
3. **Concurrent Processing** — Blocks are processed in a separate worker pool (configurable via `BLOCK_PROCESS_CONCURRENCY`)
4. **Wallet Detection** — Identifies transactions from configured CEX wallets
5. **Outflow Calculation** — Computes SOL sent from balance changes
6. **Range Matching** — Checks if amount matches configured ranges
7. **Alert Dispatch** — Sends Telegram message with transaction details

## Monitoring

The bot logs each block processed:

```
[timestamp] Processing block 384694548...
[timestamp] Processing block 384694555...
```

When an alert is triggered:

```
[timestamp] ALERT: OKX sent 19.5 SOL to <receiver>
```

## Troubleshooting

**No blocks being processed:**
- Check RPC endpoint is valid
- Verify `RPC_PRIMARY` is not rate-limited

**Telegram alerts not received:**
- Verify `TELEGRAM_BOT_TOKEN` is correct
- Verify `TELEGRAM_CHAT_ID` is correct
- Check bot has permission to send messages

**High RPC usage:**
- The bot should use minimal RPC calls (~2 per block)
- If seeing rate-limits, increase `BLOCK_POLL_MAX_MS` to 1000+

## Architecture Notes

- **No historical queries** — Uses only current block data (`getBlock`)
- **Subscriptions** — Uses WebSocket `onSlotUpdate` to receive slot notifications and eliminate polling
- **Real-time latency** — Typically <100ms from slot root to processing (depends on RPC provider)
- **Minimal state** — Only stores last processed slot number

## License

MIT

## Support

For issues or questions, open an issue on GitHub.
# Quick Download (single-file zip)

If you just want a ready-to-run zip of the current working `main` (includes compiled `dist/`):

Windows (PowerShell):

```powershell
# unzip into a folder
Expand-Archive CEX-WALLET-TRACKER-real-main.zip -DestinationPath CEX-WALLET-TRACKER
cd CEX-WALLET-TRACKER
# install deps
npm ci
# run
node dist/index.js
```

Linux / macOS:

```bash
unzip CEX-WALLET-TRACKER-real-main.zip -d CEX-WALLET-TRACKER
cd CEX-WALLET-TRACKER
npm ci
node dist/index.js
```

Notes:
- Ensure you create a `.env` file in the project root (copy from `.env.example`) and fill in `RPC_PRIMARY`, `CEX_n_ADDRESS`, and `TELEGRAM_*` if you want alerts.
- To tune concurrency, set `BLOCK_FETCH_CONCURRENCY` in `.env` (e.g., `BLOCK_FETCH_CONCURRENCY=30`).

# CEX-WALLET-TRACKER-real