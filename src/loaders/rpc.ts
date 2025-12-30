import { Connection } from '@solana/web3.js';
import { log } from '../utils/logger.js';

export class RpcManager {
  primary: Connection;
  secondary?: Connection;
  useSecondary = false;

  constructor(primaryUrl: string, secondaryUrl?: string) {
    this.primary = new Connection(primaryUrl, { commitment: 'confirmed' });
    if (secondaryUrl) this.secondary = new Connection(secondaryUrl, { commitment: 'confirmed' });
  }

  getConnection(): Connection {
    if (this.useSecondary && this.secondary) return this.secondary;
    return this.primary;
  }

  switchToSecondary() {
    if (this.secondary) {
      this.useSecondary = true;
      log('Switched to secondary RPC');
    }
  }

  switchToPrimary() {
    this.useSecondary = false;
    log('Using primary RPC');
  }
}
