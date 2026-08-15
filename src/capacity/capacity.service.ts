import {
  BadRequestException,
  ConflictException,
  Injectable,
  NotFoundException,
} from '@nestjs/common';
import { Prisma } from '@prisma/client';
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
import { PermanentEventError } from '../common/db-errors';
import { buildReleaseEvent, buildReservationEvent } from '../outbox/outbox';

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
const RETRYABLE_CODES = new Set(['40001', '40P01']);

async function withRetry<T>(fn: () => Promise<T>, max = 3): Promise<T> {
  for (let attempt = 1; ; attempt++) {
    try {
      return await fn();
    } catch (e) {
      const code =
        e instanceof Prisma.PrismaClientKnownRequestError
          ? (e.meta?.code as string | undefined)
          : undefined;
      if (attempt >= max || !code || !RETRYABLE_CODES.has(code)) throw e;
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

/// See docs/DECISIONS.md ADR-006 (pessimistic locking, FOR NO KEY UPDATE) and
/// ADR-001/003 (money representation, FX freeze).
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
    // Using the programme's scale rounded a GBP invoice to whole units for a
    // JPY programme (1234.56 -> 1235), over-reserving capacity and writing the
    // rounded figure into invoice.face_amount so the audit trail lied too.
    // See docs/DECISIONS.md ADR-020.
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
    // let the caller pick which historical rate priced its reservation, and
    // that rate is frozen onto the invoice and replayed at release, so the
    // mispricing was permanent. requestedAt survives as business metadata on
    // the ledger entry. See docs/DECISIONS.md ADR-019.
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
        if (locked.status !== 'ACTIVE') throw new ConflictException('program is not ACTIVE');

        // STEP 3 — invoice, business-key idempotency (survives past the
        // Idempotency-Key header's TTL).
        let invoice = await tx.invoice.findUnique({
          where: { programId_externalRef: { programId: program.id, externalRef: cmd.invoiceRef } },
        });
        if (invoice?.status === 'RESERVED') {
          return finishIdempotent(tx, idem, this.toView(invoice, program.currency.minorUnits));
        }
        if (invoice && invoice.status !== 'REGISTERED') {
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
            entryType: 'RESERVE',
            origin: 'API',
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
        // docs/DECISIONS.md ADR-004/005). Same transaction as the ledger
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
            status: 'RESERVED',
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

        if (invoice.status === 'RELEASED') {
          return finishIdempotent(tx, idem, {
            invoiceId: invoice.id,
            invoiceRef: cmd.invoiceRef,
            status: 'RELEASED',
          });
        }
        if (invoice.status !== 'RESERVED')
          throw new ConflictException(`invoice is ${invoice.status}`);

        // EXACTLY the amount that was reserved — never recomputed at today's
        // rate. See docs/DECISIONS.md ADR-003: this is what makes capacity
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
            entryType: 'RELEASE',
            origin: 'API',
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
        // does. Without this the release consumed a ledger seq that treasury
        // never saw, which silently broke the acknowledged_local_seq watermark
        // reconciliation depends on. See docs/DECISIONS.md ADR-022.
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
          data: { status: 'RELEASED', releasedAt: new Date(), version: { increment: 1 } },
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
      delta: { amount: string; currency: string; direction: 'RESERVE' | 'RELEASE' };
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
    // to freeze it against, contradicting ADR-003.
    //
    // So `delta.currency` is an assertion about the producer's view of the
    // world, not an instruction. A mismatch is a contract violation, and is
    // dead-lettered rather than applied. applyBaseline has always checked this
    // for snapshots; the delta path simply forgot. See ADR-028.
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
      event.delta.direction === 'RESERVE' ? event.delta.amount : `-${event.delta.amount}`;
    // Guarded exactly like the API path. Without the limit predicate this
    // UPDATE walked straight into the program_no_overcommit CHECK, which the
    // consumer then classified as transient and retried forever.
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
        origin: 'TREASURY',
        deltaReserved: signed,
        balanceReservedAfter: row.reserved_amount,
        balanceLimitAfter: row.total_limit,
        occurredAt: new Date(event.produced_at),
        eventId: event.event_id,
        // Treasury's own sequence for this event. Previously never written, so
        // the column was permanently NULL and reconciliation's
        // `treasury_event_seq > included_through_event_seq` predicate — the
        // whole TREASURY branch of the replay — could never be true. See
        // docs/DECISIONS.md ADR-026.
        treasuryEventSeq: event.event_seq ?? null,
      },
    });
  }

  /// minorUnits pins the output to the programme currency's scale (e.g.
  /// "30.00", never "30") — see docs/DECISIONS.md ADR-002. Raw
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
