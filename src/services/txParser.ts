import { ParsedTransactionWithMeta, SignatureResult } from '@solana/web3.js';

type OutgoingResult = {
  type: 'SOL';
  amount: number; // amount in SOL
  receiver?: string;
};

export function parseOutgoing(tx: ParsedTransactionWithMeta, cexAddress: string): OutgoingResult | null {
  const meta = tx.meta;
  const message = tx.transaction.message;
  if (!meta) return null;

  // Find account index for cexAddress in message.accountKeys
  const accountKeys = message.accountKeys.map((k: any) => (typeof k === 'string' ? k : k.pubkey));
  const idx = accountKeys.indexOf(cexAddress);
  if (idx === -1) return null;

  // SOL change
  if (meta.preBalances && meta.postBalances) {
    const pre = meta.preBalances[idx] ?? 0;
    const post = meta.postBalances[idx] ?? 0;
    const lamportsOut = pre - post;
    if (lamportsOut > 0) {
      // Determine receiver: find account that increased most
      let receiver: string | undefined;
      let maxDelta = 0;
      for (let i = 0; i < (meta.preBalances?.length ?? 0); i++) {
        const d = (meta.postBalances[i] ?? 0) - (meta.preBalances[i] ?? 0);
        if (d > maxDelta && i !== idx) {
          maxDelta = d;
          receiver = accountKeys[i];
        }
      }
      return { type: 'SOL', amount: lamportsOut / 1e9, receiver };
    }
  }

  // If no SOL outgoing detected, ignore (we don't process SPL)
  return null;
}
