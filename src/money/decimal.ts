import { Prisma } from '@prisma/client';

/// Cloned, NOT configured globally via Decimal.set() — Prisma.Decimal is the
/// same constructor the client uses to materialise query results, and a
/// global change would alter Prisma's own rounding behaviour. See
/// docs/DECISIONS.md ADR-02.
export const Dec = Prisma.Decimal.clone({
  precision: 34,
  rounding: Prisma.Decimal.ROUND_HALF_EVEN,
  toExpNeg: -20,
  toExpPos: 40,
});
export type Dec = InstanceType<typeof Dec>;
