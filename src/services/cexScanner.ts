import fs from 'fs/promises';
import path from 'path';
import dotenv from 'dotenv';
dotenv.config();

import { RpcManager } from '../loaders/rpc';
import { PublicKey } from '@solana/web3.js';
import { parseRanges, amountMatchesRanges } from './ranges.js';
import { parseOutgoing } from './txParser.js';
import { log, error } from '../utils/logger.js';
import { sendAlert } from '../utils/telegram.js';
import { sleep } from '../utils/sleep.js';

const DATA_DIR = path.resolve(process.cwd(), 'data');
const LAST_FILE = path.join(DATA_DIR, 'lastSignatures.json');

type CexConfig = { label: string; address: string; ranges: string };

async function ensureDataDir() {
  try {
    await fs.mkdir(DATA_DIR, { recursive: true });
  } catch (e) {}
}

async function loadLastSignatures(): Promise<Record<string, string>> {
  try {
    const raw = await fs.readFile(LAST_FILE, 'utf-8');
    return JSON.parse(raw);
  } catch (e) {
    return {};
  }
}

async function saveLastSignatures(map: Record<string, string>) {
  await ensureDataDir();
  await fs.writeFile(LAST_FILE, JSON.stringify(map, null, 2), 'utf-8');
}

function readCexConfigs(): CexConfig[] {
  const configs: CexConfig[] = [];
  const env = process.env;
  // find CEX_n_LABEL pairs
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
}export async function startScanner(rpc: RpcManager) {
  await ensureDataDir();
  const last = await loadLastSignatures();
  const configs = readCexConfigs();

  log('Starting scanner for', configs.map((c) => `${c.label}:${c.address}`).join(', '));

  for (const cfg of configs) {
    (async () => {
      // Stagger the start so multiple wallets don't hammer RPC at once
      const delay = configs.indexOf(cfg) * 5000;
      await sleep(delay);
      const ranges = parseRanges(cfg.ranges);
      let lastSig = last[cfg.address] ?? null;
      const sigLimit = Number(process.env.SIGNATURE_LIMIT ?? 10);
      const pollMin = Number(process.env.POLL_MIN_MS ?? 45000);
      const pollMax = Number(process.env.POLL_MAX_MS ?? 60000);
      const rpcTimeoutMs = Number(process.env.RPC_TIMEOUT_MS ?? 15000);

      log(`Starting polling loop for ${cfg.label} (${cfg.address})`);
      if (!lastSig) {
        try {
          const conn = rpc.getConnection();
          log(`Fetching baseline for ${cfg.label}...`);
          const sigs: any = await Promise.race([
            conn.getSignaturesForAddress(new PublicKey(cfg.address), { limit: sigLimit }),
            new Promise((_, rej) => setTimeout(() => rej(new Error('RPC timeout')), rpcTimeoutMs))
          ]);
          if (sigs && sigs.length > 0) {
            lastSig = sigs[0].signature;
            last[cfg.address] = lastSig;
            await saveLastSignatures(last);
            log(`Baseline set for ${cfg.label}: ${lastSig}`);
          }
        } catch (err) {
          error(`Initial baseline fetch failed for ${cfg.address}:`, err);
          rpc.switchToSecondary();
        }
      }

      while (true) {
        try {
          log(`[${cfg.label}] Polling...`);
          const conn = rpc.getConnection();
          const sigs = await conn.getSignaturesForAddress(new PublicKey(cfg.address), { limit: 20 });
          if (sigs && sigs.length > 0) {
            // signatures are sorted newest first
            const newSigs = [] as typeof sigs;
            for (const s of sigs) {
              if (!lastSig || s.signature === lastSig) break;
              newSigs.push(s);
            }

            if (newSigs.length > 0) {
              // process in chronological order (oldest -> newest)
              newSigs.reverse();
              for (const s of newSigs) {
                try {
                  const tx = await conn.getParsedTransaction(s.signature, 'confirmed');
                  if (!tx) continue;
                  const parsed = parseOutgoing(tx as any, cfg.address);
                  if (!parsed) continue; // only SOL outgoing are returned

                  // Log outgoing transfer
                  log(`Outgoing detected | CEX=${cfg.label} | addr=${cfg.address} | amount=${parsed.amount} | receiver=${parsed.receiver}`);

                  // check ranges (amount is in SOL)
                  const amountForCheck = parsed.amount;
                  const matched = amountMatchesRanges(amountForCheck, ranges);
                  log('Range matched?', matched);

                  if (matched && parsed.receiver) {
                    const solscanLink = `https://solscan.io/account/${parsed.receiver}`;
                    const message = `<b>🚨 Range Match Alert</b>\n<b>CEX:</b> ${cfg.label}\n<b>Amount:</b> ${parsed.amount} SOL\n<b>Receiver:</b> <a href="${solscanLink}">${parsed.receiver}</a>`;
                    await sendAlert(message);
                  }
                } catch (err) {
                  error('Error processing signature', s.signature, err);
                }
                lastSig = s.signature;
                last[cfg.address] = lastSig;
              }
              await saveLastSignatures(last);
            }
          }
        } catch (err: any) {
          error(`[${cfg.label}] Polling error:`, err?.message ?? err);
          rpc.switchToSecondary();
        }

        // sleep pollMin - pollMax randomized
        const ms = pollMin + Math.floor(Math.random() * Math.max(0, pollMax - pollMin));
        log(`[${cfg.label}] Sleeping for ${ms}ms before next poll...`);
        await sleep(ms);
      }
    })();
  }
}

export async function scanForTokenMints(_wallet: string) {
  // Placeholder for future implementation
  log('scanForTokenMints placeholder for', _wallet);
}
