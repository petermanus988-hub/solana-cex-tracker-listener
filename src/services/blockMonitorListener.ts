import fs from 'fs/promises';
import path from 'path';
import { RpcManager } from '../loaders/rpc';
import dotenv from 'dotenv';
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
  try { dotenv.config(); } catch (e) {}
    const map: Record<string, CexConfig> = {};
  const env = process.env;
  Object.keys(env).forEach((k) => {
    const m = k.match(/^CEX_(\d+)_LABEL$/);
    if (m) {
      const n = m[1];
      const rawLabel = env[`CEX_${n}_LABEL`];
      const rawAddress = env[`CEX_${n}_ADDRESS`];
      const rawRanges = env[`CEX_${n}_RANGE`];
      const label = rawLabel ? String(rawLabel).trim() : '';
      const address = rawAddress ? String(rawAddress).trim() : '';
      const ranges = rawRanges ? String(rawRanges).trim() : '';
      if (label && address) {
          map[n] = { label: String(label).trim(), address: String(address).trim(), ranges: (ranges ?? '').toString().trim() };
      } else {
        if (!label && address) log(`CEX_${n} has address but no label; skipping`);
        if (label && !address) log(`CEX_${n} has label but no address; skipping`);
      }
    }
  });
    return Object.keys(map).sort((a, b) => Number(a) - Number(b)).map((k) => map[k]);
}

export async function startBlockMonitorListener(rpc: RpcManager) {
  await ensureDataDir();
  const lastSlots = await loadLastSlot();
  const configs = readCexConfigs();

  if (configs.length === 0) {
    log('🔄 Starting listener-based block monitor — no CEX wallets configured (no CEX_N_LABEL/CEX_N_ADDRESS pairs found)');
  } else {
    log('🔄 Starting listener-based block monitor for', configs.map((c) => `${c.label}:${c.address}`).join(', '));
    for (const cfg of configs) {
      try {
        const parsedRanges = parseRanges(cfg.ranges);
        log(`🔎 Registered CEX -> label=${cfg.label} address=${cfg.address} ranges='${cfg.ranges}' parsed=${JSON.stringify(parsedRanges)}`);
      } catch (e) {
        log(`🔎 Registered CEX -> label=${cfg.label} address=${cfg.address} ranges='${cfg.ranges}' parsed=[]`);
      }
    }
  }

  const conn = rpc.getConnection();
  let lastSlot = 0;
  try {
    lastSlot = await conn.getSlot('confirmed');
    log(`🎧 Listener initialized at block ${lastSlot}, will monitor from here forward`);
  } catch (e) {
    log('Warning: Could not initialize starting slot, will start from next available');
  }

  // Separate fetch and process stages so fetching never waits on processing
  const inFlightFetches = new Set<number>();
  const processedSlots = new Map<number, boolean>(); // mark slots as processed/skipped
  const processQueue: Array<{ slot: number; blockData: any }> = [];
  let processWorkers = 0;
  const processConcurrency = Number(process.env.BLOCK_PROCESS_CONCURRENCY ?? 8);

  async function doFetch(s: number) {
    if (s <= lastSlot) return;
    if (inFlightFetches.has(s) || processedSlots.has(s)) return;
    inFlightFetches.add(s);
    try {
      log(`📦 Fetching block ${s} (fetcher)`);
      const blockData = await conn.getBlock(s, { maxSupportedTransactionVersion: 0 });
      if (!blockData || !blockData.transactions) {
        log(`Slot ${s} skipped or missing in long-term storage`);
        processedSlots.set(s, true);
        await attemptAdvance();
        return;
      }
      // enqueue for processing
      processQueue.push({ slot: s, blockData });
      tryStartProcessors();
    } catch (err: any) {
      const msg = String(err?.message ?? err);
      // treat skipped-like messages as processed
      if (msg.includes('skipped') || msg.includes('missing in long-term storage')) {
        log(`Slot ${s} skipped or missing in long-term storage`);
        processedSlots.set(s, true);
        await attemptAdvance();
        return;
      }
      // transient error (rate limit) — log and retry later (do not block fetching other slots)
      error(`Error fetching block ${s}:`, msg);
      // small delay before re-fetching
      setTimeout(() => {
        inFlightFetches.delete(s);
        doFetch(s).catch((e) => error('Refetch error', e));
      }, 500);
    } finally {
      inFlightFetches.delete(s);
    }
  }

  function tryStartProcessors() {
    while (processWorkers < processConcurrency && processQueue.length > 0) {
      const item = processQueue.shift()!;
      processBlock(item.slot, item.blockData).catch((e) => error('Processor error', e));
    }
  }

  async function processBlock(s: number, blockData: any) {
    processWorkers++;
    try {
      log(`✅ Processing block ${s}...`);
      for (const tx of blockData.transactions) {
        const signature = tx.transaction.signatures?.[0];
        if (!signature) continue;

        for (const cfg of configs) {
          const ranges = parseRanges(cfg.ranges);
          const outflows = parseBlockTransactions(tx as any, cfg.address);

          if (outflows.length > 0) {
            log(`[${cfg.label}] Found ${outflows.length} outflow(s) in tx ${signature.slice(0, 8)}...`);
          }

          for (const outflow of outflows) {
            log(`[${cfg.label}] Outflow: ${outflow.amount} SOL to ${outflow.receiver.slice(0, 8)}...`);
            if (!amountMatchesRanges(outflow.amount, ranges)) {
              log(`[${cfg.label}] Amount ${outflow.amount} SOL does NOT match ranges: ${cfg.ranges}`);
              continue;
            }

            const solscanLink = `https://solscan.io/tx/${signature}`;
            const message = `<b>🚨 Range Match Alert</b>\n<b>CEX:</b> ${cfg.label}\n<b>Amount:</b> ${outflow.amount} SOL\n<b>Receiver:</b> ${outflow.receiver}\n<b>Tx:</b> <a href="${solscanLink}">View on Solscan</a>`;
            log(`🚨 ALERT: ${cfg.label} sent ${outflow.amount} SOL to ${outflow.receiver}`);
            await sendAlert(message);
          }
        }
      }

      // mark processed and try advancing lastSlot
      processedSlots.set(s, true);
      await attemptAdvance();
    } catch (err: any) {
      error(`Error processing block ${s}:`, err?.message ?? err);
      // re-enqueue for later retry after short delay
      setTimeout(() => {
        processQueue.push({ slot: s, blockData });
        tryStartProcessors();
      }, 1000);
    } finally {
      processWorkers--;
    }
  }

  async function attemptAdvance() {
    let advanced = false;
    while (true) {
      const next = lastSlot + 1;
      if (processedSlots.has(next)) {
        lastSlot = next;
        processedSlots.delete(next);
        advanced = true;
        log(`⏭️ Advanced lastSlot to ${lastSlot}`);
        await saveLastSlot({ lastSlot });
      } else {
        break;
      }
    }
    if (advanced) {
      // if we advanced a lot, attempt to start processors in case queue built up
      tryStartProcessors();
    }
  }

  let subscriptionId: number | null = null;

  // Resilient subscription with exponential backoff on failures (e.g., 429 on ws handshake)
  let backoffMs = 500;
  let shuttingDown = false;

  async function ensureSubscription() {
    while (!shuttingDown) {
      try {
        const connToUse = rpc.getConnection();
        subscriptionId = connToUse.onSlotUpdate((slotUpdate) => {
          if (slotUpdate.type !== 'root') return; // only process rooted slots for stability
          doFetch(slotUpdate.slot).catch((e) => error('Fetch slot error', e));
        });
        log(`📡 Subscription established (id=${subscriptionId})`);
        // reset backoff on success
        backoffMs = 500;
        return;
      } catch (err: any) {
        const msg = String(err?.message ?? err);
        error('ws error on subscription:', msg);
        // Exponential backoff
        await new Promise((res) => setTimeout(res, backoffMs));
        backoffMs = Math.min(backoffMs * 2, 60000);
      }
    }
  }

  // Start subscription and keep it active
  ensureSubscription().catch((e) => error('Failed to ensure subscription', e));

  process.on('SIGINT', async () => {
    log('🛑 Received SIGINT, removing slot subscription and shutting down');
    try {
      if (subscriptionId !== null) conn.removeSlotUpdateListener(subscriptionId);
    } catch (e) {}
    shuttingDown = true;
    process.exit(0);
  });

  process.on('SIGTERM', async () => {
    log('🛑 Received SIGTERM, removing slot subscription and shutting down');
    try {
      if (subscriptionId !== null) conn.removeSlotUpdateListener(subscriptionId);
    } catch (e) {}
    shuttingDown = true;
    process.exit(0);
  });
}

export default { startBlockMonitorListener };
