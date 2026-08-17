import {
  BadRequestException,
  ConflictException,
  Injectable,
  NotFoundException,
} from '@nestjs/common';
import {
  InvoiceStatus,
  LedgerEntryType,
  LedgerOrigin,
  Prisma,
  ProgramStatus,
} from '@prisma/client';
import { randomUUID } from 'node:crypto';
import { PrismaService } from '../prisma/prisma.service';
import { FxService } from '../fx/fx.service';
import { Money } from '../money/money';
import { Dec } from '../money/decimal';
import {
  claimIdempotencyKey,
  finishIdempotent,
  IdempotencyContext,
  Tx,
} from '../idempotency/idempotency';
import { InsufficientCapacityException } from './exceptions';
import { isTransientDbError, PermanentEventError } from '../common/db-errors';
import { buildReleaseEvent, buildReservationEvent } from '../outbox/outbox';
import { DELTA_DIRECTION, type DeltaDirection } from '../kafka/schemas/capacity-event.schema';

export interface ReserveCommand {
  programRef: string;
  invoiceRef: string;
  amount: string;
  currency: string;
  requestedAt: Date;
}

export interface ReleaseCommand {
  programRef: string;
  invoiceRef: string;
}

export interface InvoiceView {
  invoiceId: string;
  invoiceRef: string;
  status: string;
  reservedAmount: string | null;
}

const TX_OPTS = {
  isolationLevel: Prisma.TransactionIsolationLevel.ReadCommitted,
  maxWait: 5_000,
  timeout: 10_000,
};
/// Contention is absorbed in-process for a few attempts before it becomes the
/// caller's problem. Classification is shared with the Kafka consumer through
/// `isTransientDbError`, so a lock timeout, a serialisation failure or a
/// deadlock cannot count as retryable on one path and fatal on the other.
/// Whatever survives all attempts is mapped to 503 by DomainExceptionFilter.
async function withRetry<T>(fn: () => Promise<T>, max = 3): Promise<T> {
  for (let attempt = 1; ; attempt++) {
    try {
      return await fn();
    } catch (e) {
      if (attempt >= max || !isTransientDbError(e)) throw e;
      await new Promise((resolve) => setTimeout(resolve, Math.random() * 25 * 2 ** attempt));
    }
  }
}

interface ProgramLockRow {
  id: string;
  currency_code: string;
  total_limit: Prisma.Decimal;
  reserved_amount: Prisma.Decimal;
  next_ledger_seq: bigint;
  status: string;
}

interface GuardedUpdateRow {
  reserved_amount: Prisma.Decimal;
  total_limit: Prisma.Decimal;
  assigned_seq: bigint;
}

/// See docs/DECISIONS.md ADR-04 (pessimistic locking, FOR NO KEY UPDATE) and
/// ADR-02 and ADR-03 (money representation, FX freeze).
@Injectable()
export class CapacityService {
  constructor(
    private readonly prisma: PrismaService,
    private readonly fx: FxService,
  ) {}

  async reserve(cmd: ReserveCommand, idem: IdempotencyContext): Promise<unknown> {
    // STEP 0 — outside the transaction: FX resolution is a read of an
    // append-only table, so doing it before the row lock keeps the lock hold
    // time to a handful of statements.
    const program = await this.prisma.program.findUniqueOrThrow({
      where: { externalRef: cmd.programRef },
      include: { currency: true },
    });

    // Quantise to the scale of the INVOICE's currency, not the programme's.
    // The programme's scale would round a GBP invoice to whole units against a
    // JPY programme (1234.56 -> 1235), over-reserving capacity and persisting
    // the rounded figure as invoice.face_amount, so the audit trail would agree
    // with the error. See docs/DECISIONS.md ADR-02.
    const currencyCode = cmd.currency.toUpperCase();
    const invoiceCurrency = await this.prisma.currency.findUnique({
      where: { code: currencyCode },
    });
    if (!invoiceCurrency) {
      throw new BadRequestException(`unknown currency: ${cmd.currency}`);
    }

    const face = Money.of(cmd.amount, currencyCode, invoiceCurrency.minorUnits);
    // Money.of quantises silently. For an inbound API amount that silence is
    // wrong: "1000.5" JPY is not a rounding opportunity, it is a malformed
    // request, and should say so rather than book a different number.
    if (!face.toDecimal().eq(new Dec(cmd.amount))) {
      throw new BadRequestException(
        `amount ${cmd.amount} carries more precision than ${currencyCode} permits ` +
          `(${invoiceCurrency.minorUnits} decimal places)`,
      );
    }

    // Server time, never cmd.requestedAt. A client-supplied valuation instant
    // would let the caller pick which historical rate prices its reservation,
    // and that rate is frozen onto the invoice and replayed at release, so the
    // mispricing would be permanent. requestedAt survives as business metadata
    // on the ledger entry. See docs/DECISIONS.md ADR-03.
    const valuedAt = new Date();
    const conversion = await this.fx.convert(
      face,
      program.currencyCode,
      program.currency.minorUnits,
      valuedAt,
    );

    return withRetry(() =>
      this.prisma.$transaction(async (tx) => {
        await tx.$executeRawUnsafe(`SET LOCAL lock_timeout = '3s'`);

        // STEP 1 — idempotency claim, first in lock order.
        const claim = await claimIdempotencyKey(tx, idem);
        if (claim.kind === 'replay') return claim.storedResponse;
        if (claim.kind === 'conflict') {
          throw new ConflictException('Idempotency-Key reused with a different request body');
        }

        // STEP 2 — lock the programme row.
        const [locked] = await tx.$queryRaw<ProgramLockRow[]>`
          SELECT id, currency_code, total_limit, reserved_amount, next_ledger_seq, status
            FROM program WHERE id = ${program.id}::uuid FOR NO KEY UPDATE`;
        if (!locked) throw new NotFoundException('program');
        if (locked.status !== ProgramStatus.ACTIVE) {
          throw new ConflictException(`program is not ${ProgramStatus.ACTIVE}`);
        }

        // STEP 3 — invoice, business-key idempotency (survives past the
        // Idempotency-Key header's TTL).
        let invoice = await tx.invoice.findUnique({
          where: { programId_externalRef: { programId: program.id, externalRef: cmd.invoiceRef } },
        });

        // Confirming an existing reservation is only sound for the SAME
        // instruction. A repeat for this invoiceRef carrying a different amount
        // or currency is a contradiction, not a replay: answering it with the
        // stored result would return 201 and the ORIGINAL figure to a caller
        // who asked for a different one, and that caller has every reason to
        // believe the amount it sent was reserved.
        if (invoice) {
          const bookedCurrency =
            invoice.currencyCode === currencyCode
              ? invoiceCurrency
              : await tx.currency.findUnique({ where: { code: invoice.currencyCode } });
          const booked = Money.fromDb(
            invoice.faceAmount,
            invoice.currencyCode,
            bookedCurrency?.minorUnits ?? invoiceCurrency.minorUnits,
          );
          if (!booked.eq(face)) {
            throw new ConflictException(
              `invoice ${cmd.invoiceRef} already exists on programme ${cmd.programRef} for ` +
                `${booked.toString()} ${booked.currency}, but this request asks for ` +
                `${face.toString()} ${face.currency}`,
            );
          }
        }

        if (invoice?.status === InvoiceStatus.RESERVED) {
          return finishIdempotent(tx, idem, this.toView(invoice, program.currency.minorUnits));
        }
        if (invoice && invoice.status !== InvoiceStatus.REGISTERED) {
          throw new ConflictException(`invoice is ${invoice.status}`);
        }
        if (!invoice) {
          invoice = await tx.invoice.create({
            data: {
              programId: program.id,
              externalRef: cmd.invoiceRef,
              faceAmount: face.toDecimal(),
              currencyCode,
            },
          });
        }

        // STEP 4 — atomic guarded debit: the capacity test lives in the
        // UPDATE's WHERE clause, so there is no read-modify-write window.
        const updated = await tx.$queryRaw<GuardedUpdateRow[]>`
          UPDATE program
             SET reserved_amount = reserved_amount + ${conversion.amount.toDecimal().toString()}::numeric,
                 next_ledger_seq = next_ledger_seq + 1,
                 version = version + 1,
                 updated_at = now()
           WHERE id = ${program.id}::uuid
             AND reserved_amount + ${conversion.amount.toDecimal().toString()}::numeric <= total_limit
          RETURNING reserved_amount, total_limit, (next_ledger_seq - 1) AS assigned_seq
        `;
        if (updated.length === 0) {
          throw new InsufficientCapacityException({
            programRef: cmd.programRef,
            requested: conversion.amount.toDecimal(),
            available: locked.total_limit.minus(locked.reserved_amount).toString(),
          });
        }
        const row = updated[0];

        // STEP 5 — append the ledger entry.
        await tx.capacityLedgerEntry.create({
          data: {
            programId: program.id,
            seq: row.assigned_seq,
            entryType: LedgerEntryType.RESERVE,
            origin: LedgerOrigin.API,
            deltaReserved: conversion.amount.toDecimal().toString(),
            deltaLimit: 0,
            balanceReservedAfter: row.reserved_amount,
            balanceLimitAfter: row.total_limit,
            invoiceId: invoice.id,
            occurredAt: cmd.requestedAt,
            idempotencyKeyId: idem.rowId,
            eventId: randomUUID(),
            metadata: {
              faceAmount: face.toJSON(),
              fx: {
                rateId: conversion.audit.rateId,
                rate: conversion.audit.effectiveRate.toString(),
                source: conversion.audit.source,
              },
            },
          },
        });

        // STEP 5b — outbox: carries this reservation's ledger seq so
        // treasury can echo it back as an acknowledgement watermark (see
        // docs/DECISIONS.md ADR-06). Same transaction as the ledger
        // append, so the two can never disagree.
        const outboxEvent = buildReservationEvent({
          programRef: cmd.programRef,
          invoiceRef: cmd.invoiceRef,
          seq: row.assigned_seq,
          amount: conversion.amount.toString(),
          currency: program.currencyCode,
          occurredAt: cmd.requestedAt,
        });
        await tx.outboxMessage.create({
          data: {
            topic: outboxEvent.topic,
            key: outboxEvent.key,
            payload: outboxEvent.payload as Prisma.InputJsonValue,
          },
        });

        // STEP 6 — freeze FX onto the invoice.
        const finalInvoice = await tx.invoice.update({
          where: { id: invoice.id },
          data: {
            status: InvoiceStatus.RESERVED,
            reservedProgramAmount: conversion.amount.toDecimal().toString(),
            fxRateId: conversion.audit.rateId,
            fxRate: conversion.audit.effectiveRate.toString(),
            fxRateSource: conversion.audit.source as Prisma.InvoiceUpdateInput['fxRateSource'],
            fxRateAsOf: conversion.audit.asOf,
            fxInverted: conversion.audit.inverted,
            fxPivot: conversion.audit.pivot,
            reservedAt: new Date(),
            version: { increment: 1 },
          },
        });

        // STEP 7 — seal the idempotency key in the same transaction as the
        // money move: a crash in between is impossible, so a replay can
        // never re-execute.
        return finishIdempotent(tx, idem, this.toView(finalInvoice, program.currency.minorUnits));
      }, TX_OPTS),
    );
  }

  async release(cmd: ReleaseCommand, idem: IdempotencyContext): Promise<unknown> {
    const program = await this.prisma.program.findUniqueOrThrow({
      where: { externalRef: cmd.programRef },
      include: { currency: true },
    });

    return withRetry(() =>
      this.prisma.$transaction(async (tx) => {
        await tx.$executeRawUnsafe(`SET LOCAL lock_timeout = '3s'`);

        const claim = await claimIdempotencyKey(tx, idem);
        if (claim.kind === 'replay') return claim.storedResponse;
        if (claim.kind === 'conflict') {
          throw new ConflictException('Idempotency-Key reused with a different request body');
        }

        const [locked] = await tx.$queryRaw<{ id: string; status: string }[]>`
          SELECT id, status FROM program WHERE id = ${program.id}::uuid FOR NO KEY UPDATE`;
        if (!locked) throw new NotFoundException('program');

        const [invoice] = await tx.$queryRaw<
          { id: string; status: string; reserved_program_amount: Prisma.Decimal | null }[]
        >`
          SELECT id, status, reserved_program_amount FROM invoice
           WHERE program_id = ${program.id}::uuid AND external_ref = ${cmd.invoiceRef} FOR NO KEY UPDATE`;
        if (!invoice) throw new NotFoundException('invoice');

        if (invoice.status === InvoiceStatus.RELEASED) {
          return finishIdempotent(tx, idem, {
            invoiceId: invoice.id,
            invoiceRef: cmd.invoiceRef,
            status: InvoiceStatus.RELEASED,
          });
        }
        if (invoice.status !== InvoiceStatus.RESERVED) {
          throw new ConflictException(`invoice is ${invoice.status}`);
        }

        // EXACTLY the amount that was reserved — never recomputed at today's
        // rate. See docs/DECISIONS.md ADR-03: this is what makes capacity
        // return to precisely its prior level.
        const amount = invoice.reserved_program_amount!;

        const updated = await tx.$queryRaw<
          { reserved_amount: Prisma.Decimal; total_limit: Prisma.Decimal; assigned_seq: bigint }[]
        >`
          UPDATE program
             SET reserved_amount = reserved_amount - ${amount.toString()}::numeric,
                 next_ledger_seq = next_ledger_seq + 1, version = version + 1, updated_at = now()
           WHERE id = ${program.id}::uuid AND reserved_amount - ${amount.toString()}::numeric >= 0
          RETURNING reserved_amount, total_limit, (next_ledger_seq - 1) AS assigned_seq
        `;
        if (updated.length === 0) {
          throw new ConflictException(
            'materialised balance would go negative — data integrity issue',
          );
        }
        const row = updated[0];
        const releasedAt = new Date();

        await tx.capacityLedgerEntry.create({
          data: {
            programId: program.id,
            seq: row.assigned_seq,
            entryType: LedgerEntryType.RELEASE,
            origin: LedgerOrigin.API,
            deltaReserved: amount.negated().toString(),
            deltaLimit: 0,
            balanceReservedAfter: row.reserved_amount,
            balanceLimitAfter: row.total_limit,
            invoiceId: invoice.id,
            occurredAt: releasedAt,
            idempotencyKeyId: idem.rowId,
            eventId: randomUUID(),
          },
        });

        // Same transaction as the ledger append, exactly as the reserve path
        // does. Without it the release consumes a ledger seq that treasury
        // never sees, silently breaking the acknowledged_local_seq watermark
        // reconciliation depends on. See docs/DECISIONS.md ADR-09.
        const outboxEvent = buildReleaseEvent({
          programRef: cmd.programRef,
          invoiceRef: cmd.invoiceRef,
          seq: row.assigned_seq,
          amount: amount.toString(),
          currency: program.currencyCode,
          occurredAt: releasedAt,
        });
        await tx.outboxMessage.create({
          data: {
            topic: outboxEvent.topic,
            key: outboxEvent.key,
            payload: outboxEvent.payload as Prisma.InputJsonValue,
          },
        });

        const finalInvoice = await tx.invoice.update({
          where: { id: invoice.id },
          data: {
            status: InvoiceStatus.RELEASED,
            releasedAt: new Date(),
            version: { increment: 1 },
          },
        });

        return finishIdempotent(tx, idem, this.toView(finalInvoice, program.currency.minorUnits));
      }, TX_OPTS),
    );
  }

  async getAvailability(programRef: string): Promise<{
    programRef: string;
    currency: string;
    totalLimit: string;
    reservedAmount: string;
    available: string;
  }> {
    const program = await this.prisma.program.findUnique({
      where: { externalRef: programRef },
      include: { currency: true },
    });
    if (!program) throw new NotFoundException('program');
    const minorUnits = program.currency.minorUnits;
    return {
      programRef: program.externalRef,
      currency: program.currencyCode,
      totalLimit: program.totalLimit.toFixed(minorUnits),
      reservedAmount: program.reservedAmount.toFixed(minorUnits),
      available: program.totalLimit.minus(program.reservedAmount).toFixed(minorUnits),
    };
  }

  /// Applied by the Kafka consumer for small, incremental treasury-origin
  /// deltas — same lock/ledger discipline as the API path, different origin
  /// tag. Runs INSIDE the caller's transaction (the consumer's inbox tx).
  async applyTreasuryDelta(
    tx: Tx,
    event: {
      program_ref: string;
      event_id: string;
      produced_at: string;
      event_seq?: bigint | null;
      delta: { amount: string; currency: string; direction: DeltaDirection };
    },
  ): Promise<void> {
    const program = await tx.program.findUnique({
      where: { externalRef: event.program_ref },
    });
    if (!program) {
      throw new PermanentEventError(`unknown program ${event.program_ref}`);
    }

    // The delta modifies program.reserved_amount, a column denominated in the
    // programme's currency, so the amount must already be in that currency —
    // there is nothing else it could sensibly be. The event carries no rate,
    // and converting at one of ours would book an FX residue with no invoice
    // to freeze it against, contradicting ADR-03.
    //
    // So `delta.currency` is an assertion about the producer's view of the
    // world, not an instruction. A mismatch is a contract violation, and is
    // dead-lettered rather than applied — the same check applyBaseline makes
    // for snapshots. See ADR-06.
    const deltaCurrency = event.delta.currency.toUpperCase();
    if (deltaCurrency !== program.currencyCode) {
      throw new PermanentEventError(
        `treasury delta for ${event.program_ref} is denominated in ${deltaCurrency}, ` +
          `but the programme is ${program.currencyCode}`,
      );
    }

    const [locked] = await tx.$queryRaw<{ id: string }[]>`
      SELECT id FROM program WHERE id = ${program.id}::uuid FOR NO KEY UPDATE`;
    if (!locked) throw new PermanentEventError(`program ${event.program_ref} vanished under lock`);

    const signed =
      event.delta.direction === DELTA_DIRECTION.RESERVE
        ? event.delta.amount
        : `-${event.delta.amount}`;
    // Guarded exactly like the API path. Without the limit predicate this
    // UPDATE walks straight into the program_no_overcommit CHECK, which the
    // consumer classifies as transient and would retry forever.
    const updated = await tx.$queryRaw<
      { reserved_amount: Prisma.Decimal; total_limit: Prisma.Decimal; assigned_seq: bigint }[]
    >`
      UPDATE program SET reserved_amount = reserved_amount + ${signed}::numeric,
             next_ledger_seq = next_ledger_seq + 1, version = version + 1, updated_at = now()
       WHERE id = ${program.id}::uuid
         AND reserved_amount + ${signed}::numeric >= 0
         AND (reserved_amount + ${signed}::numeric <= total_limit OR over_commit_acknowledged)
      RETURNING reserved_amount, total_limit, (next_ledger_seq - 1) AS assigned_seq
    `;
    if (updated.length === 0) {
      throw new PermanentEventError(
        `treasury delta ${event.event_id} (${signed} ${deltaCurrency}) would move ` +
          `${event.program_ref} outside [0, total_limit]`,
      );
    }
    const row = updated[0];
    await tx.capacityLedgerEntry.create({
      data: {
        programId: program.id,
        seq: row.assigned_seq,
        entryType: event.delta.direction,
        origin: LedgerOrigin.TREASURY,
        deltaReserved: signed,
        balanceReservedAfter: row.reserved_amount,
        balanceLimitAfter: row.total_limit,
        occurredAt: new Date(event.produced_at),
        eventId: event.event_id,
        // Treasury's own sequence for this event. Reconciliation's
        // `treasury_event_seq > included_through_event_seq` predicate — the
        // whole TREASURY branch of the replay — compares it against the
        // snapshot watermark, so a NULL here leaves that branch unsatisfiable.
        // See docs/DECISIONS.md ADR-06.
        treasuryEventSeq: event.event_seq ?? null,
      },
    });
  }

  /// minorUnits pins the output to the programme currency's scale (e.g.
  /// "30.00", never "30") — see docs/DECISIONS.md ADR-02. Raw
  /// Prisma.Decimal#toString() does not preserve trailing zeros, so it must
  /// not be called directly on money fields headed for an API response.
  private toView(
    invoice: {
      id: string;
      externalRef: string;
      status: string;
      reservedProgramAmount: Prisma.Decimal | null;
    },
    minorUnits: number,
  ): InvoiceView {
    return {
      invoiceId: invoice.id,
      invoiceRef: invoice.externalRef,
      status: invoice.status,
      reservedAmount: invoice.reservedProgramAmount?.toFixed(minorUnits) ?? null,
    };
  }
}
