export type Range = { min: number; max: number };

export function parseRanges(rangeStr: string | undefined): Range[] {
  if (!rangeStr) return [];
  return rangeStr
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean)
    .map((token) => {
      const parts = token.split('-').map((p) => p.trim());
      const a = parts[0];
      const b = parts[1];
      const min = Number(a);
      const max = b !== undefined ? Number(b) : Number(a);
      if (Number.isNaN(min) || Number.isNaN(max)) return null as any;
      return { min, max } as Range;
    })
    .filter(Boolean) as Range[];
}

export function amountMatchesRanges(amount: number, ranges: Range[]) {
  for (const r of ranges) {
    if (amount >= r.min && amount <= r.max) return true;
  }
  return false;
}
