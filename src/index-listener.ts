import dotenv from 'dotenv';
dotenv.config();

import { RpcManager } from './loaders/rpc.js';
import { startTransactionMonitorListener } from './services/transactionMonitorListener.js';
import { log, error } from './utils/logger.js';

const RPC_PRIMARY = (process.env.RPC_PRIMARY || '').trim();
const RPC_SECONDARY = (process.env.RPC_SECONDARY || '').trim();

if (!RPC_PRIMARY) {
  console.error('RPC_PRIMARY is not configured in .env');
  process.exit(1);
}

const rpc = new RpcManager(RPC_PRIMARY, RPC_SECONDARY || undefined);

async function main() {
  try {
    log('🚀 Starting Solana CEX Transaction Monitor (V2 WebSocket-Native)');
    await startTransactionMonitorListener(rpc);
  } catch (err) {
    error('Fatal error in main (listener)', err);
    process.exit(1);
  }
}

process.on('SIGINT', () => {
  log('Received SIGINT, shutting down');
  process.exit(0);
});
process.on('SIGTERM', () => {
  log('Received SIGTERM, shutting down');
  process.exit(0);
});

main();
