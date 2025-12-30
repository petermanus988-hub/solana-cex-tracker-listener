import { VersionedTransaction, PublicKey } from '@solana/web3.js';
import bs58 from 'bs58';

type Outflow = { amount: number; receiver: string; type: 'SOL' };

// System Program pubkey
const SYSTEM_PROGRAM = '11111111111111111111111111111111';

export function parseBlockTransactions(tx: any, cexAddress: string): Outflow[] {
  const results: Outflow[] = [];
  const meta = tx.meta;
  if (!meta) return results;

  const message = tx.transaction.message;
  const accountKeys = message.staticAccountKeys || message.accountKeys || [];

  // Process each instruction looking for System Program transfers
  const instructions = message.instructions || [];

  for (const instr of instructions) {
    // --- Case 1: Parsed Instruction (from getParsedTransaction) ---
    if (instr.parsed && instr.program === 'system') {
      try {
        const parsed = instr.parsed;

        // Check if it's a transfer instruction
        if (parsed.type === 'transfer') {
          const info = parsed.info;
          if (!info) continue;

          // Check if CEX is the sender (source)
          if (info.source === cexAddress && info.lamports > 0) {
            results.push({
              amount: info.lamports / 1e9,
              receiver: info.destination,
              type: 'SOL'
            });
          }
        }
        continue;
      } catch (e) {
        continue;
      }
    }

    // --- Case 2: Raw Instruction (from getTransaction or getBlock) ---
    try {
      // Get program ID
      const programIdx = instr.programIdIndex;
      if (programIdx === undefined) continue;

      const programId = accountKeys[programIdx];
      const programAddr = typeof programId === 'string' ? programId : programId?.toBase58?.();

      // Only care about System Program transfers
      if (programAddr !== SYSTEM_PROGRAM) continue;

      // Decode instruction data (it's Base58-encoded)
      let instrData: Buffer;
      if (typeof instr.data === 'string') {
        instrData = Buffer.from(bs58.decode(instr.data));
      } else if (instr.data instanceof Uint8Array) {
        instrData = Buffer.from(instr.data);
      } else {
        continue;
      }

      // Check for transfer instruction discriminator [2, 0, 0, 0]
      if (instrData.length < 12) continue;
      if (instrData[0] !== 2 || instrData[1] !== 0 || instrData[2] !== 0 || instrData[3] !== 0) continue;

      // Extract amount (bytes 4-12, little-endian u64)
      const amountBuf = instrData.slice(4, 12);
      const transferAmountLamports = Number(amountBuf.readBigUInt64LE());

      if (transferAmountLamports <= 0) continue;

      // Get from and to account indices
      const accounts = instr.accounts || [];
      if (accounts.length < 2) continue;

      const fromIdx = accounts[0];
      const toIdx = accounts[1];

      // Check if FROM account is our CEX
      const fromAddr = accountKeys[fromIdx];
      const fromAddrStr = typeof fromAddr === 'string' ? fromAddr : fromAddr?.toBase58?.();

      if (fromAddrStr !== cexAddress) continue;

      // Get receiver address
      const toAddr = accountKeys[toIdx];
      const receiver = typeof toAddr === 'string' ? toAddr : toAddr?.toBase58?.() || String(toAddr);

      if (receiver) {
        results.push({
          amount: transferAmountLamports / 1e9,
          receiver,
          type: 'SOL'
        });
      }
    } catch (e) {
      // Silently skip malformed instructions
      continue;
    }
  }

  return results;
}




