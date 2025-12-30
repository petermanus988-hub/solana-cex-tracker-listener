import fs from 'fs/promises';
import path from 'path';
import { PublicKey } from '@solana/web3.js';
import { RpcManager } from '../loaders/rpc';
import { parseRanges, amountMatchesRanges } from './ranges.js';
import { parseBlockTransactions } from './blockParser.js';
import { log, error } from '../utils/logger.js';
import { sendAlert } from '../utils/telegram.js';

const DATA_DIR = path.resolve(process.cwd(), 'data');
const LAST_SLOT_FILE = path.join(DATA_DIR, 'lastSlot.json');

type CexConfig = { label: string; address: string; ranges: string };

async function ensureDataDir() {
  try {
    await fs.mkdir(DATA_DIR, { recursive: true });
  } catch (e) {}
}

async function loadLastSlot(): Promise<Record<string, number>> {
  try {
    const raw = await fs.readFile(LAST_SLOT_FILE, 'utf-8');
    return JSON.parse(raw);
  } catch (e) {
    return {};
  }
}

async function saveLastSlot(map: Record<string, number>) {
  await ensureDataDir();
  await fs.writeFile(LAST_SLOT_FILE, JSON.stringify(map, null, 2), 'utf-8');
}

function readCexConfigs(): CexConfig[] {
  const configs: CexConfig[] = [];
  const env = process.env;
  Object.keys(env).forEach((k) => {
    const m = k.match(/^CEX_(\d+)_LABEL$/);
    if (m) {
      const n = m[1];
      const label = env[`CEX_${n}_LABEL`];
      const address = env[`CEX_${n}_ADDRESS`];
      const ranges = env[`CEX_${n}_RANGE`];
      if (label && address) {
        configs.push({ label, address, ranges: ranges ?? '' });
      }
    }
  });
  return configs;
}

export async function startTransactionMonitorListener(rpc: RpcManager) {
  await ensureDataDir();
  const lastSlots = await loadLastSlot();
  const configs = readCexConfigs();

  log('🚀 Starting V2 WebSocket-native transaction monitor for', configs.map((c) => `${c.label}:${c.address}`).join(', '));

  const conn = rpc.getConnection();
  let lastSlot = 0;
  try {
    lastSlot = await conn.getSlot('confirmed');
    log(`🎧 Listener initialized at slot ${lastSlot}, will monitor from here forward`);
  } catch (e) {
    log('Warning: Could not initialize starting slot, will start from next available');
  }

  // Track processed transactions to avoid duplicates
  const processedTxs = new Set<string>();
  const processedTxsMaxSize = 10000; // Keep memory bounded

  let shuttingDown = false;
  const subscriptionIds: number[] = [];

  // Create onLogs subscription for each CEX wallet
  async function setupSubscriptions() {
    for (const cfg of configs) {
      try {
        const cexPubkey = new PublicKey(cfg.address);
        log(`📡 Setting up onLogs subscription for ${cfg.label} (${cfg.address})`);

        const subscriptionId = conn.onLogs(
          cexPubkey,
          async (logResult) => {
            try {
              const signature = logResult.signature;
              
              // Skip if already processed
              if (processedTxs.has(signature)) {
                return;
              }
              processedTxs.add(signature);
              
              // Keep set bounded
              if (processedTxs.size > processedTxsMaxSize) {
                const arr = Array.from(processedTxs);
                arr.slice(0, processedTxsMaxSize / 2).forEach(tx => processedTxs.delete(tx));
              }

              log(`🔗 Detected transaction for ${cfg.label}: ${signature.slice(0, 8)}...`);

              // Fetch the parsed transaction with retry logic
              let tx = null;
              let retries = 3;
              let lastError: any = null;
              
              while (retries > 0) {
                try {
                  tx = await conn.getParsedTransaction(signature, {
                    maxSupportedTransactionVersion: 0,
                    commitment: 'confirmed'
                  });
                  if (tx) break; // Success - exit retry loop
                  
                  // If null but no error, it might be not yet available
                  retries--;
                  if (retries > 0) {
                    await new Promise(res => setTimeout(res, 500));
                  }
                } catch (err: any) {
                  lastError = err;
                  retries--;
                  if (retries > 0) {
                    // Wait before retrying
                    await new Promise(res => setTimeout(res, 500));
                  }
                }
              }

              if (!tx) {
                error(`Failed to fetch transaction ${signature.slice(0, 8)}... after 3 retries: ${lastError?.message ?? 'Unknown error'}`);
                return;
              }

              // Create a transaction object compatible with blockParser
              const txWrapper = {
                transaction: tx.transaction,
                meta: tx.meta
              };

              // Parse outflows using existing logic
              const ranges = parseRanges(cfg.ranges);
              const outflows = parseBlockTransactions(txWrapper as any, cfg.address);

              if (outflows.length > 0) {
                // Log all outflows with amounts immediately
                for (const outflow of outflows) {
                  log(`[${cfg.label}] Found ${outflows.length} outflow(s) in tx ${signature.slice(0, 8)}...`);
                  log(`[${cfg.label}] Outflow: ${outflow.amount} SOL to ${outflow.receiver.slice(0, 8)}...`);
                }
              }

              // Check each outflow against configured ranges
              for (const outflow of outflows) {
                const shortSig = signature.slice(0, 6);
                
                if (!amountMatchesRanges(outflow.amount, ranges)) {
                  log(`[${cfg.label}] 📤 ${outflow.amount} SOL does not match range (${cfg.ranges})`);
                  continue;
                }

                // Range match found - show full details
                const solscanLink = `https://solscan.io/tx/${signature}`;
                const message = `<b>🚨 Range Match Alert</b>\n<b>CEX:</b> ${cfg.label}\n<b>Amount:</b> ${outflow.amount} SOL\n<b>Receiver:</b> <code>${outflow.receiver}</code>\n<b>Tx:</b> <code>${signature}</code>\n\n<a href="${solscanLink}">View on Solscan</a>`;
                log(`🚨 ALERT: ${cfg.label} sent ${outflow.amount} SOL to ${outflow.receiver}`);
                
                await sendAlert(message);
              }

              // Update last processed slot
              if (tx.slot) {
                if (!lastSlots[cfg.label] || tx.slot > lastSlots[cfg.label]) {
                  lastSlots[cfg.label] = tx.slot;
                  await saveLastSlot(lastSlots);
                }
              }
            } catch (err: any) {
              error(`Error processing log for ${cfg.label}:`, err?.message ?? err);
            }
          },
          'confirmed' // Only listen to confirmed transactions
        );

        subscriptionIds.push(subscriptionId);
        log(`✅ Subscription established for ${cfg.label} (id=${subscriptionId})`);
      } catch (err: any) {
        error(`Failed to setup subscription for ${cfg.label}:`, err?.message ?? err);
      }
    }
  }

  // Setup subscriptions and keep them active with exponential backoff on failure
  let backoffMs = 500;
  async function ensureSubscriptions() {
    while (!shuttingDown) {
      try {
        // Clear old subscriptions if any
        for (const id of subscriptionIds) {
          try {
            conn.removeOnLogsListener(id);
          } catch (e) {}
        }
        subscriptionIds.length = 0;

        await setupSubscriptions();
        
        if (subscriptionIds.length === 0) {
          throw new Error('No subscriptions were established');
        }

        // Reset backoff on success
        backoffMs = 500;
        return;
      } catch (err: any) {
        const msg = String(err?.message ?? err);
        error('WebSocket error on subscription:', msg);
        
        // Exponential backoff
        await new Promise((res) => setTimeout(res, backoffMs));
        backoffMs = Math.min(backoffMs * 2, 60000);
      }
    }
  }

  // Start subscriptions
  ensureSubscriptions().catch((e) => error('Failed to ensure subscriptions', e));

  // Handle graceful shutdown
  process.on('SIGINT', async () => {
    log('🛑 Received SIGINT, removing subscriptions and shutting down');
    shuttingDown = true;
    try {
      for (const id of subscriptionIds) {
        conn.removeOnLogsListener(id);
      }
    } catch (e) {}
    process.exit(0);
  });

  process.on('SIGTERM', async () => {
    log('🛑 Received SIGTERM, removing subscriptions and shutting down');
    shuttingDown = true;
    try {
      for (const id of subscriptionIds) {
        conn.removeOnLogsListener(id);
      }
    } catch (e) {}
    process.exit(0);
  });
}

export default { startTransactionMonitorListener };
