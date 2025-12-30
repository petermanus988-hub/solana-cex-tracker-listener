export type Range = { min: number; max: number };

export function parseRanges(rangeStr: string | undefined): Range[] {
  if (!rangeStr) return [];
  return rangeStr
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean)
    .map((token) => {
      const [a, b] = token.split('-');
      const min = Number(a);
      const max = Number(b);
      return { min, max };
    });
}

export function amountMatchesRanges(amount: number, ranges: Range[]) {
  for (const r of ranges) {
    if (amount >= r.min && amount <= r.max) return true;
  }
  return false;
}
