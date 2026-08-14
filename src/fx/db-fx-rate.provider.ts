import { Injectable } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';
import { Dec } from '../money/decimal';
import { FxRateProvider, FxRateUnavailableError, ResolvedRate } from './fx-rate.provider';

const PIVOT = process.env.FX_PIVOT_CURRENCY ?? 'USD';

@Injectable()
export class DbFxRateProvider implements FxRateProvider {
  constructor(private readonly prisma: PrismaService) {}

  async resolve(base: string, quote: string, at: Date): Promise<ResolvedRate> {
    const direct = await this.prisma.fxRate.findFirst({
      where: { baseCurrencyCode: base, quoteCurrencyCode: quote, asOf: { lte: at } },
      orderBy: { asOf: 'desc' },
    });
    if (direct) {
      return {
        rateId: direct.id,
        rate: new Dec(direct.rate.toString()),
        inverted: false,
        pivot: null,
        source: direct.source,
        asOf: direct.asOf,
      };
    }

    const inverse = await this.prisma.fxRate.findFirst({
      where: { baseCurrencyCode: quote, quoteCurrencyCode: base, asOf: { lte: at } },
      orderBy: { asOf: 'desc' },
    });
    if (inverse) {
      return {
        rateId: inverse.id,
        rate: new Dec(inverse.rate.toString()),
        inverted: true,
        pivot: null,
        source: inverse.source,
        asOf: inverse.asOf,
      };
    }

    if (base !== PIVOT && quote !== PIVOT) {
      const [toBase, toQuote] = await Promise.all([
        this.prisma.fxRate.findFirst({
          where: { baseCurrencyCode: PIVOT, quoteCurrencyCode: base, asOf: { lte: at } },
          orderBy: { asOf: 'desc' },
        }),
        this.prisma.fxRate.findFirst({
          where: { baseCurrencyCode: PIVOT, quoteCurrencyCode: quote, asOf: { lte: at } },
          orderBy: { asOf: 'desc' },
        }),
      ]);
      if (toBase && toQuote) {
        const rate = new Dec(toQuote.rate.toString()).div(toBase.rate.toString());
        return {
          rateId: toQuote.id,
          rate,
          inverted: false,
          pivot: PIVOT,
          source: toQuote.source,
          asOf: toQuote.asOf,
        };
      }
    }

    throw new FxRateUnavailableError(
      `No rate available for ${base}->${quote} at ${at.toISOString()}`,
    );
  }
}
