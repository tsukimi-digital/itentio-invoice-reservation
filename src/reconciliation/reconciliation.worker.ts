import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Prisma } from '@prisma/client';
import { PrismaService } from '../prisma/prisma.service';
import { Dec } from '../money/decimal';

export function computeBaseline(input: {
  treasuryReserved: Dec;
  replayedSum: Dec;
  localReservedBefore: Dec;
}): {
  newReserved: Dec;
  baselineDelta: Dec;
} {
  const newReserved = input.treasuryReserved.plus(input.replayedSum);
  const baselineDelta = newReserved.minus(input.localReservedBefore);
  return { newReserved, baselineDelta };
}

interface JobHeader {
  event_id: string;
  correlation_id?: string;
  acknowledged_local_seq: string | null;
  included_through_event_seq: string | null;
  program_currency: string;
}

export function nextCursor(page: { invoiceRef: string }[]): string | null {
  return page.length === 0 ? null : page[page.length - 1].invoiceRef;
}

/// See docs/DECISIONS.md ADR-004/005 (baseline+replay, bidirectional
/// acknowledgement watermark) and ADR-011 (overcommit after reconciliation
/// is flagged, never clamped).
@Injectable()
export class ReconciliationWorker implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(ReconciliationWorker.name);
  private timer?: NodeJS.Timeout;
  private readonly workerId = `worker-${process.pid}-${Math.random().toString(36).slice(2, 8)}`;

  constructor(private readonly prisma: PrismaService) {}

  onModuleInit(): void {
    this.timer = setInterval(() => void this.tick(), 2_000);
  }

  onModuleDestroy(): void {
    if (this.timer) clearInterval(this.timer);
  }

  private async tick(): Promise<void> {
    const job = await this.claimNextJob();
    if (!job) return;
    try {
      // priorStatus, not job.status: claimNextJob's own UPDATE already moved
      // status to CLAIMED before this row was returned, so job.status here is
      // always 'CLAIMED' and cannot be used to decide which phase to run.
      if (job.priorStatus === 'PENDING') await this.applyBaseline(job);
      const refreshed = await this.prisma.reconciliationJob.findUniqueOrThrow({
        where: { id: job.id },
      });
      // A job reaches this point via two different routes, and both must
      // trigger position sync: (a) applyBaseline just ran in THIS tick and
      // moved it PENDING -> BASELINE_APPLIED (refreshed.status reflects
      // that), or (b) the job ARRIVED already BASELINE_APPLIED from an
      // earlier tick — e.g. position sync was interrupted by a restart and
      // resumed via claimNextJob picking it up again. In case (b),
      // claimNextJob's own claim already stamped refreshed.status to
      // 'CLAIMED', so only job.priorStatus still remembers where it came
      // from. Checking refreshed.status alone (as an earlier draft did)
      // silently drops every resumed job — caught by a smoke test that
      // seeded a job directly at BASELINE_APPLIED.
      if (refreshed.status === 'BASELINE_APPLIED' || job.priorStatus === 'BASELINE_APPLIED') {
        await this.syncPositions(refreshed);
      }
    } catch (err) {
      this.logger.error(`Reconciliation job ${job.id} failed: ${(err as Error).message}`);
      await this.prisma.reconciliationJob.update({
        where: { id: job.id },
        data: { status: 'FAILED', lastError: (err as Error).message },
      });
    }
  }

  /// Claims the next eligible job and returns it tagged with its PRE-claim
  /// status (SELECT-then-UPDATE in one transaction, so the SKIP LOCKED read
  /// and the claiming UPDATE stay atomic) — RETURNING on the UPDATE alone
  /// would only ever report the post-update 'CLAIMED' status, which is not
  /// enough to tell applyBaseline and syncPositions apart in tick().
  private async claimNextJob() {
    return this.prisma.$transaction(async (tx) => {
      const [row] = await tx.$queryRaw<{ id: string; status: string }[]>`
        SELECT id, status FROM reconciliation_job
         WHERE status IN ('PENDING', 'BASELINE_APPLIED')
           AND available_at <= now() AND chunks_received = chunk_count
         ORDER BY snapshot_seq LIMIT 1 FOR UPDATE SKIP LOCKED
      `;
      if (!row) return null;

      await tx.reconciliationJob.update({
        where: { id: row.id },
        data: {
          status: 'CLAIMED',
          claimedAt: new Date(),
          claimedBy: this.workerId,
          attempts: { increment: 1 },
        },
      });
      const job = await tx.reconciliationJob.findUniqueOrThrow({ where: { id: row.id } });
      return { ...job, priorStatus: row.status as 'PENDING' | 'BASELINE_APPLIED' };
    });
  }

  private async applyBaseline(job: {
    id: string;
    programRef: string;
    snapshotId: string;
    snapshotSeq: bigint;
    asOf: Date;
    header: unknown;
  }): Promise<void> {
    const header = job.header as JobHeader & { total_limit: string; reserved_amount: string };

    await this.prisma.$transaction(
      async (tx) => {
        await tx.$executeRawUnsafe(`SET LOCAL lock_timeout = '5s'`);

        const program = await tx.program.findUniqueOrThrow({
          where: { externalRef: job.programRef },
        });
        const [p] = await tx.$queryRaw<
          {
            id: string;
            currency_code: string;
            total_limit: Prisma.Decimal;
            reserved_amount: Prisma.Decimal;
            treasury_snapshot_seq: bigint | null;
          }[]
        >`
        SELECT id, currency_code, total_limit, reserved_amount, treasury_snapshot_seq
          FROM program WHERE id = ${program.id}::uuid FOR NO KEY UPDATE`;

        if (p.treasury_snapshot_seq !== null && p.treasury_snapshot_seq >= job.snapshotSeq) {
          await tx.reconciliationJob.update({
            where: { id: job.id },
            data: { status: 'SUPERSEDED' },
          });
          return;
        }
        if (p.currency_code !== header.program_currency) {
          throw new Error(
            `snapshot currency ${header.program_currency} does not match program currency ${p.currency_code}`,
          );
        }

        const ackSeq = header.acknowledged_local_seq ? BigInt(header.acknowledged_local_seq) : 0n;
        const includedSeq = header.included_through_event_seq
          ? BigInt(header.included_through_event_seq)
          : 0n;

        const [agg] = await tx.$queryRaw<{ sum_delta: Prisma.Decimal; hi: bigint; n: bigint }[]>`
        SELECT COALESCE(SUM(delta_reserved), 0) AS sum_delta, COALESCE(MAX(seq), 0) AS hi, COUNT(*) AS n
          FROM capacity_ledger_entry
         WHERE program_id = ${p.id}::uuid
           AND ((origin = 'API' AND seq > ${ackSeq}) OR (origin = 'TREASURY' AND treasury_event_seq > ${includedSeq}))
      `;

        const { newReserved, baselineDelta } = computeBaseline({
          treasuryReserved: new Dec(header.reserved_amount),
          replayedSum: new Dec(agg.sum_delta.toString()),
          localReservedBefore: new Dec(p.reserved_amount.toString()),
        });
        const treasuryLimit = new Dec(header.total_limit);
        const overCommitted = newReserved.gt(treasuryLimit);
        if (overCommitted) {
          await tx.reconciliationDiscrepancy.create({
            data: {
              jobId: job.id,
              programId: p.id,
              kind: 'OVER_LIMIT_AFTER_BASELINE',
              expected: treasuryLimit.toString(),
              actual: newReserved.toString(),
              detail: { note: 'treasury limit below reserved capacity; flagged, not clamped' },
            },
          });
        }

        const applied = await tx.$queryRaw<{ assigned_seq: bigint }[]>`
        UPDATE program
           SET total_limit = ${treasuryLimit.toString()}::numeric, reserved_amount = ${newReserved.toString()}::numeric,
               over_commit_acknowledged = ${overCommitted},
               treasury_snapshot_seq = ${job.snapshotSeq}, treasury_baseline_as_of = ${job.asOf},
               treasury_acked_local_seq = ${ackSeq}, treasury_included_event_seq = ${includedSeq},
               next_ledger_seq = next_ledger_seq + 1, version = version + 1, updated_at = now()
         WHERE id = ${p.id}::uuid AND (treasury_snapshot_seq IS NULL OR treasury_snapshot_seq < ${job.snapshotSeq})
        RETURNING (next_ledger_seq - 1) AS assigned_seq
      `;
        if (applied.length === 0) {
          await tx.reconciliationJob.update({
            where: { id: job.id },
            data: { status: 'SUPERSEDED' },
          });
          return;
        }

        await tx.capacityLedgerEntry.create({
          data: {
            programId: p.id,
            seq: applied[0].assigned_seq,
            entryType: 'RECONCILE_BASELINE',
            origin: 'RECONCILIATION',
            deltaReserved: baselineDelta.toString(),
            deltaLimit: treasuryLimit.minus(p.total_limit.toString()).toString(),
            balanceReservedAfter: newReserved.toString(),
            balanceLimitAfter: treasuryLimit.toString(),
            occurredAt: job.asOf,
            eventId: header.event_id,
            metadata: {
              snapshotId: job.snapshotId,
              snapshotSeq: job.snapshotSeq.toString(),
              treasuryReserved: header.reserved_amount,
              localReservedBefore: p.reserved_amount.toString(),
              replayedCount: Number(agg.n),
              replayedSum: agg.sum_delta.toString(),
              replayedThroughSeq: agg.hi.toString(),
              acknowledgedLocalSeq: ackSeq.toString(),
            },
          },
        });

        // Stamp the resolved internal program id onto the job now — syncPositions
        // (Task 12) needs it for discrepancy records and must not reuse job.id,
        // which is the job's own identity, not the program's.
        await tx.reconciliationJob.update({
          where: { id: job.id },
          data: { status: 'BASELINE_APPLIED', programId: p.id },
        });
      },
      { isolationLevel: Prisma.TransactionIsolationLevel.ReadCommitted, timeout: 15_000 },
    );
  }

  /// The capacity number is already correct and durable after applyBaseline;
  /// this phase only reconciles per-invoice attribution and emits
  /// discrepancies, so it can safely be interrupted and resumed via
  /// positionCursor — no transaction here spans more than one bounded chunk.
  private async syncPositions(job: {
    id: string;
    programId: string | null;
    positionCursor: string | null;
  }): Promise<void> {
    const CHUNK = 500;
    let cursor = job.positionCursor;
    // programId is stamped by applyBaseline before status ever reaches
    // BASELINE_APPLIED (see Task 11), so this is never null in practice —
    // the guard exists because the Prisma column is nullable.
    if (!job.programId) {
      throw new Error(`reconciliation job ${job.id} has no programId — baseline must run first`);
    }
    const programId = job.programId;

    for (;;) {
      const done = await this.prisma.$transaction(
        async (tx) => {
          const page = await tx.reconciliationSnapshotPosition.findMany({
            where: { jobId: job.id, ...(cursor ? { invoiceRef: { gt: cursor } } : {}) },
            orderBy: { invoiceRef: 'asc' },
            take: CHUNK,
          });
          if (page.length === 0) return true;

          for (const pos of page) {
            const invoice = await tx.invoice.findFirst({ where: { externalRef: pos.invoiceRef } });
            if (!invoice) {
              await tx.reconciliationDiscrepancy.create({
                data: {
                  jobId: job.id,
                  programId,
                  kind: 'UNKNOWN_INVOICE',
                  invoiceRef: pos.invoiceRef,
                  detail: { snapshotAmount: pos.reservedAmount.toString() },
                },
              });
              continue;
            }
            if (
              invoice.status === 'RESERVED' &&
              invoice.reservedProgramAmount?.toString() !== pos.reservedAmount.toString()
            ) {
              await tx.reconciliationDiscrepancy.create({
                data: {
                  jobId: job.id,
                  programId,
                  kind: 'AMOUNT_DRIFT',
                  invoiceRef: pos.invoiceRef,
                  expected: pos.reservedAmount,
                  actual: invoice.reservedProgramAmount,
                  detail: { snapshotCurrency: pos.currencyCode, invoiceStatus: invoice.status },
                },
              });
            }
          }

          cursor = nextCursor(page);
          await tx.reconciliationJob.update({
            where: { id: job.id },
            data: { positionCursor: cursor },
          });
          return page.length < CHUNK;
        },
        { timeout: 15_000 },
      );

      if (done) break;
    }

    await this.prisma.reconciliationJob.update({
      where: { id: job.id },
      data: { status: 'DONE', completedAt: new Date() },
    });
  }
}
