import { ConflictException, Injectable, NotFoundException } from '@nestjs/common';
import { Prisma } from '@prisma/client';
import { randomUUID } from 'node:crypto';
import { PrismaService } from '../prisma/prisma.service';
import { FxService } from '../fx/fx.service';
import { Money } from '../money/money';
import {
  claimIdempotencyKey,
  finishIdempotent,
  IdempotencyContext,
} from '../idempotency/idempotency';
import { InsufficientCapacityException } from './exceptions';

export interface ReserveCommand {
  programRef: string;
  invoiceRef: string;
  amount: string;
  currency: string;
  requestedAt: Date;
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

    const face = Money.of(cmd.amount, cmd.currency, program.currency.minorUnits);
    const targetCurrency = await this.prisma.currency.findUniqueOrThrow({
      where: { code: program.currencyCode },
    });
    const conversion = await this.fx.convert(
      face,
      program.currencyCode,
      targetCurrency.minorUnits,
      cmd.requestedAt,
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
          return finishIdempotent(tx, idem, this.toView(invoice, targetCurrency.minorUnits));
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
              currencyCode: cmd.currency,
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
        return finishIdempotent(tx, idem, this.toView(finalInvoice, targetCurrency.minorUnits));
      }, TX_OPTS),
    );
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
