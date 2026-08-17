import { Inject, Injectable } from '@nestjs/common';
import { Money } from '../money/money';
import { Dec } from '../money/decimal';
import { FX_RATE_PROVIDER, FxRateProvider } from './fx-rate.provider';

export interface FxConversion {
  amount: Money;
  audit: {
    rateId: string | null;
    effectiveRate: Dec;
    source: string;
    asOf: Date;
    inverted: boolean;
    pivot: string | null;
  };
}

/// The single point of rounding in the whole system. See docs/DECISIONS.md
/// ADR-02: HALF_EVEN, applied exactly once, here.
@Injectable()
export class FxService {
  constructor(@Inject(FX_RATE_PROVIDER) private readonly provider: FxRateProvider) {}

  async convert(
    from: Money,
    toCurrency: string,
    toMinorUnits: number,
    at: Date,
  ): Promise<FxConversion> {
    if (from.currency === toCurrency) {
      return {
        amount: from,
        audit: {
          rateId: null,
          effectiveRate: new Dec(1),
          source: 'IDENTITY',
          asOf: at,
          inverted: false,
          pivot: null,
        },
      };
    }

    const resolved = await this.provider.resolve(from.currency, toCurrency, at);

    // Divide, don't multiply by a materialised inverse: one rounding, not two.
    const raw = resolved.inverted
      ? from.toDecimal().div(resolved.rate)
      : from.toDecimal().times(resolved.rate);

    return {
      amount: Money.of(raw, toCurrency, toMinorUnits),
      audit: {
        rateId: resolved.rateId,
        effectiveRate: resolved.rate,
        source: resolved.source,
        asOf: resolved.asOf,
        inverted: resolved.inverted,
        pivot: resolved.pivot,
      },
    };
  }
}
