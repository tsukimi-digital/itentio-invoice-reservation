import { Transform, Type } from 'class-transformer';
import { IsDate, IsString, Length, Matches, MaxDate } from 'class-validator';

/// Positive decimal, at most 18 integer digits and 4 decimal places.
///
/// - The `(?=.*[1-9])` lookahead rejects "0", "0.00" and "0.0000".
/// - No sign is accepted, so "-100.00" cannot reach the service. It used to:
///   `@IsNumberString()` accepts negatives, the guarded UPDATE computed
///   `500 + (-100) <= 1000` and happily *lowered* reserved_amount, and only a
///   CHECK constraint three statements later stopped it — as a 500.
/// - 18 integer digits keeps every intermediate sum inside numeric(24,4).
/// - 4 decimal places is the column's scale; the per-currency scale (0 for
///   JPY, 2 for GBP) is enforced in CapacityService against the invoice
///   currency's `minor_units`.
const POSITIVE_MONEY = /^(?=.*[1-9])\d{1,18}(\.\d{1,4})?$/;

/// External references land in VarChar(64) columns. Unbounded input reached
/// Postgres and came back as a 500 rather than a 400.
export const EXTERNAL_REF = /^[A-Za-z0-9][A-Za-z0-9._\-/]{0,63}$/;

export class ReserveDto {
  @IsString()
  @Matches(EXTERNAL_REF, {
    message: 'invoiceRef must be 1-64 chars of letters, digits, dot, dash, underscore or slash',
  })
  invoiceRef!: string;

  @IsString()
  @Matches(POSITIVE_MONEY, {
    message:
      'amount must be a positive decimal with at most 18 integer digits and 4 decimal places',
  })
  amount!: string;

  @IsString()
  @Length(3, 3)
  @Matches(/^[A-Za-z]{3}$/, { message: 'currency must be a 3-letter ISO-4217 code' })
  @Transform(({ value }: { value: unknown }): unknown =>
    typeof value === 'string' ? value.toUpperCase() : value,
  )
  currency!: string;

  /// Business metadata only — it is recorded as the ledger entry's
  /// `occurred_at`. It deliberately no longer selects the FX rate (ADR-019),
  /// but a future-dated value would still corrupt the audit trail.
  @Type(() => Date)
  @IsDate()
  @MaxDate(() => new Date(), { message: 'requestedAt must not be in the future' })
  requestedAt!: Date;
}
