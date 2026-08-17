import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import type { FxRate } from '@prisma/client';
import { PrismaService } from '../prisma/prisma.service';
import { Dec } from '../money/decimal';
import { FxRateProvider, FxRateUnavailableError, ResolvedRate } from './fx-rate.provider';
import type { Env } from '../config/env.schema';

const MS_PER_HOUR = 60 * 60 * 1000;

@Injectable()
export class DbFxRateProvider implements FxRateProvider {
  constructor(
    private readonly prisma: PrismaService,
    private readonly config: ConfigService<Env, true>,
  ) {}

  private get pivot(): string {
    return this.config.get('FX_PIVOT_CURRENCY', { infer: true });
  }

  private get maxAgeMs(): number {
    return this.config.get('FX_MAX_RATE_AGE_HOURS', { infer: true }) * MS_PER_HOUR;
  }

  /// `at` is the valuation instant. It is supplied by the caller as *server*
  /// time — never a client-controlled timestamp. A request body that picked
  /// the valuation instant would choose which historical rate prices the
  /// reservation, and the chosen rate is then frozen onto the invoice and
  /// replayed at release, so the mispricing would be permanent.
  async resolve(base: string, quote: string, at: Date): Promise<ResolvedRate> {
    const direct = await this.newestUsable(base, quote, at);
    if (direct) {
      this.assertFresh(direct, at, `${base}->${quote}`);
      return {
        rateId: direct.id,
        rate: new Dec(direct.rate.toString()),
        inverted: false,
        pivot: null,
        source: direct.source,
        asOf: direct.asOf,
      };
    }

    const inverse = await this.newestUsable(quote, base, at);
    if (inverse) {
      this.assertFresh(inverse, at, `${quote}->${base} (inverted)`);
      return {
        rateId: inverse.id,
        rate: new Dec(inverse.rate.toString()),
        inverted: true,
        pivot: null,
        source: inverse.source,
        asOf: inverse.asOf,
      };
    }

    const pivot = this.pivot;
    if (base !== pivot && quote !== pivot) {
      const [toBase, toQuote] = await Promise.all([
        this.newestUsable(pivot, base, at),
        this.newestUsable(pivot, quote, at),
      ]);
      if (toBase && toQuote) {
        this.assertFresh(toBase, at, `${pivot}->${base} (cross leg)`);
        this.assertFresh(toQuote, at, `${pivot}->${quote} (cross leg)`);
        const rate = new Dec(toQuote.rate.toString()).div(toBase.rate.toString());
        return {
          rateId: toQuote.id,
          rate,
          inverted: false,
          // The cross is only as fresh as its stalest leg.
          pivot,
          source: toQuote.source,
          asOf: toBase.asOf < toQuote.asOf ? toBase.asOf : toQuote.asOf,
        };
      }
    }

    throw new FxRateUnavailableError(
      `No rate available for ${base}->${quote} at ${at.toISOString()}`,
    );
  }

  /// Newest rate that is not in the future and whose explicit validity window
  /// (if any) still covers `at` — `valid_until` bounds a rate's usable life
  /// and is honoured here, the only place it is read.
  private newestUsable(base: string, quote: string, at: Date): Promise<FxRate | null> {
    return this.prisma.fxRate.findFirst({
      where: {
        baseCurrencyCode: base,
        quoteCurrencyCode: quote,
        asOf: { lte: at },
        OR: [{ validUntil: null }, { validUntil: { gt: at } }],
      },
      orderBy: { asOf: 'desc' },
    });
  }

  /// A row surviving `newestUsable` can still be arbitrarily old — nothing in
  /// the data model forces a rate to be superseded. Without this bound a
  /// three-year-old rate would price a reservation with no error and no
  /// warning.
  private assertFresh(rate: FxRate, at: Date, label: string): void {
    const ageMs = at.getTime() - rate.asOf.getTime();
    if (ageMs > this.maxAgeMs) {
      const ageHours = Math.round(ageMs / MS_PER_HOUR);
      throw new FxRateUnavailableError(
        `Rate ${label} is stale: as of ${rate.asOf.toISOString()} (${ageHours}h old), ` +
          `maximum accepted age is ${this.config.get('FX_MAX_RATE_AGE_HOURS', { infer: true })}h`,
      );
    }
  }
}
