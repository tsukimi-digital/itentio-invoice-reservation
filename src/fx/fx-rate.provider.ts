import type { FxRateSource } from '@prisma/client';
import type { Dec } from '../money/decimal';

export interface ResolvedRate {
  rateId: string;
  rate: Dec;
  inverted: boolean;
  pivot: string | null;
  source: FxRateSource;
  asOf: Date;
}

export class FxRateUnavailableError extends Error {}

export interface FxRateProvider {
  resolve(base: string, quote: string, at: Date): Promise<ResolvedRate>;
}

export const FX_RATE_PROVIDER = Symbol('FX_RATE_PROVIDER');
