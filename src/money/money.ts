import { Dec } from './decimal';
import type { Prisma } from '@prisma/client';

export class CurrencyMismatchError extends Error {
  constructor(a: string, b: string) {
    super(`Currency mismatch: ${a} vs ${b}`);
  }
}

/// Immutable. Always quantised to its currency's minor units on construction,
/// so an invalid amount can never exist.
export class Money {
  private constructor(
    private readonly value: Dec,
    readonly currency: string,
    readonly minorUnits: number,
  ) {}

  static of(amount: string | Dec, currency: string, minorUnits: number): Money {
    return new Money(
      new Dec(amount).toDecimalPlaces(minorUnits, Dec.ROUND_HALF_EVEN),
      currency.toUpperCase(),
      minorUnits,
    );
  }

  static zero(currency: string, minorUnits: number): Money {
    return Money.of('0', currency, minorUnits);
  }

  static fromDb(amount: Prisma.Decimal, currency: string, minorUnits: number): Money {
    return new Money(new Dec(amount.toString()), currency.toUpperCase(), minorUnits);
  }

  private assertSame(other: Money): void {
    if (other.currency !== this.currency) {
      throw new CurrencyMismatchError(this.currency, other.currency);
    }
  }

  plus(other: Money): Money {
    this.assertSame(other);
    return new Money(this.value.plus(other.value), this.currency, this.minorUnits);
  }

  minus(other: Money): Money {
    this.assertSame(other);
    return new Money(this.value.minus(other.value), this.currency, this.minorUnits);
  }

  negated(): Money {
    return new Money(this.value.negated(), this.currency, this.minorUnits);
  }

  gt(other: Money): boolean {
    this.assertSame(other);
    return this.value.gt(other.value);
  }

  gte(other: Money): boolean {
    this.assertSame(other);
    return this.value.gte(other.value);
  }

  eq(other: Money): boolean {
    return this.currency === other.currency && this.value.eq(other.value);
  }

  isZero(): boolean {
    return this.value.isZero();
  }

  toDecimal(): Dec {
    return this.value;
  }

  toString(): string {
    return this.value.toFixed(this.minorUnits);
  }

  toJSON(): { amount: string; currency: string } {
    return { amount: this.toString(), currency: this.currency };
  }
}
