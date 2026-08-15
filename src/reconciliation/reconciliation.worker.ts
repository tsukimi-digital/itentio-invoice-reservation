import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Prisma, ReconciliationStatus } from '@prisma/client';
import { randomUUID } from 'node:crypto';
import { PrismaService } from '../prisma/prisma.service';
import { Dec } from '../money/decimal';

const TICK_INTERVAL_MS = 2_000;
const CHUNK = 500;

/// A CLAIMED job whose worker has not touched it for this long is assumed
/// dead and may be re-claimed. `claimNextJob` commits status = CLAIMED and
/// then releases the row lock, so a SIGKILL mid-`applyBaseline` used to leave
/// the job in a state that matched no selector — invisible to every worker,
/// forever. See docs/DECISIONS.md ADR-029.
const CLAIM_LEASE_MS = 5 * 60 * 1000;

/// Attempts before a job stops being retried and waits for a human.
const MAX_JOB_ATTEMPTS = 5;

/// Clock-skew allowance for the time-based replay fallback. Treasury's `as_of`
/// and our `occurred_at` come from different clocks, and ADR-004/005 commits
/// to erring towards over-replay: over-replaying understates available
/// capacity (a recoverable false rejection), under-replaying overstates it
/// (a real overcommit).
const REPLAY_SKEW_MS = 5 * 60 * 1000;

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

/// Exponential backoff, capped, for a job that failed transiently.
export function backoffMs(attempts: number): number {
  return Math.min(2 ** attempts * 1_000, 5 * 60 * 1000);
}

interface ClaimedJob {
  id: string;
  programId: string | null;
  programRef: string;
  snapshotId: string;
  snapshotSeq: bigint;
  asOf: Date;
  header: Prisma.JsonValue;
  attempts: number;
  positionCursor: string | null;
  phase: 'BASELINE' | 'POSITIONS';
}

/// See docs/DECISIONS.md ADR-004/005 (baseline+replay, bidirectional
/// acknowledgement watermark) and ADR-011 (overcommit after reconciliation
/// is flagged, never clamped).
@Injectable()
export class ReconciliationWorker implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(ReconciliationWorker.name);
  private timer?: NodeJS.Timeout;
  private running = false;
  private stopping = false;
  private inFlight: Promise<void> = Promise.resolve();
  private readonly workerId = `worker-${process.pid}-${randomUUID().slice(0, 8)}`;

  constructor(private readonly prisma: PrismaService) {}

  onModuleInit(): void {
    this.timer = setInterval(() => {
      void this.tick();
    }, TICK_INTERVAL_MS);
  }

  async onModuleDestroy(): Promise<void> {
    this.stopping = true;
    if (this.timer) clearInterval(this.timer);
    await this.inFlight;
  }

  /// Re-entrancy guard. `syncPositions` runs outside any transaction across
  /// many chunks; for a large snapshot that takes far longer than the tick
  /// interval, and the next tick used to re-claim the very same job and run a
  /// second position sync concurrently — duplicating discrepancy rows and
  /// letting `positionCursor` move backwards. On one instance.
  private async tick(): Promise<void> {
    if (this.running || this.stopping) return;
    this.running = true;
    this.inFlight = this.runOnce()
      .catch((err: unknown) => {
        this.logger.error(`Reconciliation tick failed: ${String(err)}`);
      })
      .finally(() => {
        this.running = false;
      });
    await this.inFlight;
  }

  private async runOnce(): Promise<void> {
    const job = await this.claimNextJob();
    if (!job) return;
    try {
      if (job.phase === 'BASELINE') {
        await this.applyBaseline(job);
      }
      const refreshed = await this.prisma.reconciliationJob.findUniqueOrThrow({
        where: { id: job.id },
      });
      if (refreshed.status === 'BASELINE_APPLIED' || job.phase === 'POSITIONS') {
        await this.syncPositions({
          id: refreshed.id,
          programId: refreshed.programId,
          positionCursor: refreshed.positionCursor,
        });
      }
    } catch (err) {
      await this.requeueOrFail(job, err);
    }
  }

  /// Returns a job to the queue with a backoff, or parks it as FAILED once
  /// attempts are exhausted.
  ///
  /// Previously any throw set FAILED terminally, and FAILED matched no
  /// selector — so a single `SET LOCAL lock_timeout` trip, caused by a long
  /// reservation holding the programme row, permanently abandoned that
  /// programme's snapshot with nothing alerting on it.
  private async requeueOrFail(job: ClaimedJob, err: unknown): Promise<void> {
    const reason = err instanceof Error ? err.message : String(err);

    if (job.attempts >= MAX_JOB_ATTEMPTS) {
      this.logger.error(
        `Reconciliation job ${job.id} (${job.programRef}) parked as FAILED after ` +
          `${job.attempts} attempts: ${reason}`,
      );
      await this.prisma.reconciliationJob.update({
        where: { id: job.id },
        data: { status: ReconciliationStatus.FAILED, lastError: reason },
      });
      return;
    }

    // Where to resume from is decided by the durable state, not by what this
    // attempt intended: applyBaseline stamps programId in the same
    // transaction that sets BASELINE_APPLIED, so a non-null programId means
    // the baseline is already committed and must not run again.
    const fresh = await this.prisma.reconciliationJob.findUnique({ where: { id: job.id } });
    const resumeAt = fresh?.programId
      ? ReconciliationStatus.BASELINE_APPLIED
      : ReconciliationStatus.PENDING;

    this.logger.warn(
      `Reconciliation job ${job.id} (${job.programRef}) attempt ${job.attempts} failed, ` +
        `retrying as ${resumeAt}: ${reason}`,
    );
    await this.prisma.reconciliationJob.update({
      where: { id: job.id },
      data: {
        status: resumeAt,
        lastError: reason,
        availableAt: new Date(Date.now() + backoffMs(job.attempts)),
      },
    });
  }

  /// Claims the next eligible job, including one abandoned by a dead worker.
  ///
  /// `chunks_received >= chunk_count`, not `=`: a producer that resends a
  /// chunk under a fresh event_id pushes the counter past the total, and
  /// equality then never held again, leaving a complete snapshot parked
  /// forever. `>=` degrades to "may run with a duplicate chunk counted",
  /// which costs only per-invoice attribution — the capacity figure itself
  /// comes from the snapshot header, not from the positions.
  private async claimNextJob(): Promise<ClaimedJob | null> {
    return this.prisma.$transaction(async (tx) => {
      const leaseCutoff = new Date(Date.now() - CLAIM_LEASE_MS);
      const [row] = await tx.$queryRaw<{ id: string; status: string; program_id: string | null }[]>`
        SELECT id, status, program_id FROM reconciliation_job
         WHERE chunks_received >= chunk_count
           AND (
                 (status IN ('PENDING', 'BASELINE_APPLIED') AND available_at <= now())
              OR (status = 'CLAIMED' AND claimed_at IS NOT NULL AND claimed_at < ${leaseCutoff})
           )
         ORDER BY snapshot_seq LIMIT 1 FOR UPDATE SKIP LOCKED
      `;
      if (!row) return null;

      const updated = await tx.reconciliationJob.update({
        where: { id: row.id },
        data: {
          status: ReconciliationStatus.CLAIMED,
          claimedAt: new Date(),
          claimedBy: this.workerId,
          attempts: { increment: 1 },
        },
      });

      // A re-claimed CLAIMED job carries no record of which phase it died in,
      // so derive it the same way requeueOrFail does.
      const priorPhase: ClaimedJob['phase'] =
        row.status === 'BASELINE_APPLIED' || (row.status === 'CLAIMED' && row.program_id !== null)
          ? 'POSITIONS'
          : 'BASELINE';

      if (row.status === 'CLAIMED') {
        this.logger.warn(
          `Re-claiming job ${row.id} abandoned by a previous worker; resuming at ${priorPhase}`,
        );
      }

      return {
        id: updated.id,
        programId: updated.programId,
        programRef: updated.programRef,
        snapshotId: updated.snapshotId,
        snapshotSeq: updated.snapshotSeq,
        asOf: updated.asOf,
        header: updated.header,
        attempts: updated.attempts,
        positionCursor: updated.positionCursor,
        phase: priorPhase,
      };
    });
  }

  private async applyBaseline(job: ClaimedJob): Promise<void> {
    const header = job.header as unknown as JobHeader & {
      total_limit: string;
      reserved_amount: string;
    };

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
            data: { status: ReconciliationStatus.SUPERSEDED },
          });
          return;
        }
        if (p.currency_code !== header.program_currency) {
          throw new Error(
            `snapshot currency ${header.program_currency} does not match program currency ${p.currency_code}`,
          );
        }

        const ackKnown = header.acknowledged_local_seq !== null;
        const includedKnown = header.included_through_event_seq !== null;
        const ackSeq = ackKnown ? BigInt(header.acknowledged_local_seq!) : 0n;
        const includedSeq = includedKnown ? BigInt(header.included_through_event_seq!) : 0n;
        // ADR-004/005 promised this fallback and never implemented it: a
        // missing watermark used to mean ackSeq = 0, i.e. replay every API
        // entry ever recorded, which double-counts the entire history into a
        // snapshot that already contains it and can freeze the programme.
        const timeCutoff = new Date(job.asOf.getTime() - REPLAY_SKEW_MS);

        const [agg] = await tx.$queryRaw<{ sum_delta: Prisma.Decimal; hi: bigint; n: bigint }[]>`
        SELECT COALESCE(SUM(delta_reserved), 0) AS sum_delta, COALESCE(MAX(seq), 0) AS hi, COUNT(*) AS n
          FROM capacity_ledger_entry
         WHERE program_id = ${p.id}::uuid
           AND CASE
                 WHEN origin = 'API' THEN
                   CASE WHEN ${ackKnown}::boolean THEN seq > ${ackSeq}
                        ELSE occurred_at > ${timeCutoff}
                   END
                 WHEN origin = 'TREASURY' THEN
                   -- treasury_event_seq IS NULL means "we cannot tell whether
                   -- treasury folded this in". It was ALWAYS null before
                   -- ADR-026, so this branch never matched and every treasury
                   -- delta was silently erased by the baseline.
                   CASE WHEN ${includedKnown}::boolean AND treasury_event_seq IS NOT NULL
                        THEN treasury_event_seq > ${includedSeq}
                        ELSE occurred_at > ${timeCutoff}
                   END
                 ELSE false
               END
      `;

        const { newReserved, baselineDelta } = computeBaseline({
          treasuryReserved: new Dec(header.reserved_amount),
          replayedSum: new Dec(agg.sum_delta.toString()),
          localReservedBefore: new Dec(p.reserved_amount.toString()),
        });
        const treasuryLimit = new Dec(header.total_limit);
        const overCommitted = newReserved.gt(treasuryLimit);

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
            data: { status: ReconciliationStatus.SUPERSEDED },
          });
          return;
        }

        // Recorded only after the UPDATE actually applied, so a superseded
        // snapshot cannot leave an orphaned discrepancy behind.
        if (overCommitted) {
          await this.recordDiscrepancy(tx, {
            jobId: job.id,
            programId: p.id,
            programRef: job.programRef,
            kind: 'OVER_LIMIT_AFTER_BASELINE',
            expected: treasuryLimit.toString(),
            actual: newReserved.toString(),
            detail: { note: 'treasury limit below reserved capacity; flagged, not clamped' },
          });
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
              acknowledgedLocalSeq: ackKnown ? ackSeq.toString() : null,
              includedThroughEventSeq: includedKnown ? includedSeq.toString() : null,
              replayStrategy: ackKnown ? 'watermark' : 'time-fallback',
              replayTimeCutoff: ackKnown ? null : timeCutoff.toISOString(),
            },
          },
        });

        // Stamp the resolved internal program id onto the job now — syncPositions
        // needs it for discrepancy records, and requeueOrFail reads it to decide
        // which phase a retry resumes from.
        await tx.reconciliationJob.update({
          where: { id: job.id },
          data: { status: ReconciliationStatus.BASELINE_APPLIED, programId: p.id },
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
    let cursor = job.positionCursor;
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
            // Scoped to the programme. invoice.external_ref is unique only per
            // programme, so an unscoped findFirst matched an arbitrary
            // same-ref invoice belonging to a DIFFERENT programme — raising a
            // drift alert against the wrong figures and leaking another
            // programme's amounts into this one's discrepancy record. That
            // record is now readable over HTTP, which turns the old bug into a
            // cross-programme disclosure. See docs/DECISIONS.md ADR-030.
            const invoice = await tx.invoice.findUnique({
              where: { programId_externalRef: { programId, externalRef: pos.invoiceRef } },
            });
            if (!invoice) {
              await this.recordDiscrepancy(tx, {
                jobId: job.id,
                programId,
                kind: 'UNKNOWN_INVOICE',
                invoiceRef: pos.invoiceRef,
                detail: { snapshotAmount: pos.reservedAmount.toString() },
              });
              continue;
            }
            if (invoice.currencyCode !== pos.currencyCode) {
              // Comparing amounts across currencies is meaningless; say so
              // rather than emit a nonsensical AMOUNT_DRIFT.
              await this.recordDiscrepancy(tx, {
                jobId: job.id,
                programId,
                kind: 'CURRENCY_MISMATCH',
                invoiceRef: pos.invoiceRef,
                detail: {
                  snapshotCurrency: pos.currencyCode,
                  invoiceCurrency: invoice.currencyCode,
                },
              });
              continue;
            }
            if (
              invoice.status === 'RESERVED' &&
              // Decimal comparison, not string comparison: ADR-001 itself warns
              // that toString() does not preserve trailing zeros.
              !(invoice.reservedProgramAmount?.equals(pos.reservedAmount) ?? false)
            ) {
              await this.recordDiscrepancy(tx, {
                jobId: job.id,
                programId,
                kind: 'AMOUNT_DRIFT',
                invoiceRef: pos.invoiceRef,
                expected: pos.reservedAmount,
                actual: invoice.reservedProgramAmount,
                detail: { snapshotCurrency: pos.currencyCode, invoiceStatus: invoice.status },
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
      data: { status: ReconciliationStatus.DONE, completedAt: new Date() },
    });
  }

  /// Discrepancies used to be written and read by absolutely nothing — no
  /// endpoint, no log, no alert — while ADR-011 claimed they were "visible and
  /// auditable". They are now queryable (ReconciliationController) and every
  /// one of them announces itself in the log.
  private async recordDiscrepancy(
    tx: Prisma.TransactionClient,
    input: {
      jobId: string;
      programId: string;
      programRef?: string;
      kind: string;
      invoiceRef?: string;
      expected?: Prisma.Decimal | string | null;
      actual?: Prisma.Decimal | string | null;
      detail: Prisma.InputJsonValue;
    },
  ): Promise<void> {
    this.logger.warn(
      `Reconciliation discrepancy ${input.kind} on program ${input.programRef ?? input.programId}` +
        (input.invoiceRef ? ` invoice ${input.invoiceRef}` : '') +
        (input.expected != null ? ` expected=${input.expected.toString()}` : '') +
        (input.actual != null ? ` actual=${input.actual.toString()}` : ''),
    );
    await tx.reconciliationDiscrepancy.create({
      data: {
        jobId: input.jobId,
        programId: input.programId,
        kind: input.kind,
        invoiceRef: input.invoiceRef ?? null,
        expected: input.expected ?? null,
        actual: input.actual ?? null,
        detail: input.detail,
      },
    });
  }
}
