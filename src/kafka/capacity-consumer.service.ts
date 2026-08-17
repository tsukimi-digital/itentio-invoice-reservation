import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Consumer, Kafka, KafkaMessage } from 'kafkajs';
import { createHash } from 'node:crypto';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../prisma/prisma.service';
import { CapacityService } from '../capacity/capacity.service';
import { capacityEventEnvelope, EVENT_TYPE, SnapshotEvent } from './schemas/capacity-event.schema';
import { DeadLetterService } from './dead-letter.service';
import { isTransientDbError } from '../common/db-errors';
import type { Env } from '../config/env.schema';
import type { Tx } from '../idempotency/idempotency';

const TREASURY_TOPIC = 'treasury.capacity-events';
/// How many times the same offset may fail with a *transient* error before it
/// is parked in the dead-letter table anyway. Without an upper bound, a
/// misclassified error still blocks the partition indefinitely — the exact
/// failure mode this whole classification exists to prevent.
const MAX_TRANSIENT_ATTEMPTS = 5;
const CONNECT_RETRY_MS = 5_000;

/// At-least-once consumption + idempotent DB writes, never Kafka EOS. A bulk
/// snapshot is never applied inline here; it's parked as a ReconciliationJob
/// and applied by a separate
/// worker in bounded chunks, so a long reconciliation never risks a
/// consumer rebalance (kafkajs has no max.poll.interval.ms — heartbeat()
/// between every message is what keeps this consumer alive under load).
@Injectable()
export class CapacityConsumerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(CapacityConsumerService.name);
  private kafka!: Kafka;
  private consumer?: Consumer;
  private stopping = false;
  /// Transient-failure counter per (topic, partition, offset). In memory on
  /// purpose: it bounds a redelivery storm, and losing the count on restart
  /// simply grants a fresh set of attempts.
  private readonly transientAttempts = new Map<string, number>();

  /// Resolves once the consumer has joined its group and started its runner.
  /// Exposed so an integration test can produce only after the subscription
  /// exists (`fromBeginning: false` would otherwise skip anything published
  /// before the join), and so a readiness probe has something real to await.
  private markReady!: () => void;
  readonly ready: Promise<void> = new Promise<void>((resolve) => {
    this.markReady = resolve;
  });

  constructor(
    private readonly prisma: PrismaService,
    private readonly capacity: CapacityService,
    private readonly dlq: DeadLetterService,
    private readonly config: ConfigService<Env, true>,
  ) {}

  onModuleInit(): void {
    this.kafka = new Kafka({
      clientId: this.config.get('KAFKA_CLIENT_ID', { infer: true }),
      brokers: this.config.get('KAFKA_BROKERS', { infer: true }),
    });

    // Started in the background and never awaited: a rejection from
    // `admin.connect()` inside onModuleInit would abort NestFactory.create and
    // take the entire HTTP API down with it — including reserve, release and
    // availability, which touch only Postgres. Kafka is the deferrable part.
    void this.startWithRetry();
  }

  async onModuleDestroy(): Promise<void> {
    this.stopping = true;
    await this.consumer?.disconnect().catch(() => undefined);
  }

  private async startWithRetry(): Promise<void> {
    while (!this.stopping) {
      try {
        await this.start();
        this.logger.log(`Consuming ${TREASURY_TOPIC}`);
        this.markReady();
        return;
      } catch (err) {
        this.logger.warn(
          `Kafka consumer start failed, retrying in ${CONNECT_RETRY_MS}ms: ${String(err)}`,
        );
        await new Promise((resolve) => setTimeout(resolve, CONNECT_RETRY_MS));
      }
    }
  }

  private async start(): Promise<void> {
    // A consumer subscribing to a topic nothing has ever produced to races
    // the broker's auto-create and throws an uncaught (though "retriable")
    // UNKNOWN_TOPIC_OR_PARTITION. In a real deployment the treasury system
    // provisions this topic out-of-band; locally, nothing does — so this
    // service self-provisions it defensively. createTopics() is idempotent
    // (a no-op if the topic already exists), so this is always safe to run.
    const admin = this.kafka.admin();
    await admin.connect();
    try {
      await admin.createTopics({ topics: [{ topic: TREASURY_TOPIC, numPartitions: 1 }] });
    } catch (err) {
      this.logger.warn(`Could not pre-create ${TREASURY_TOPIC}: ${(err as Error).message}`);
    } finally {
      await admin.disconnect();
    }

    const consumer = this.kafka.consumer({
      groupId: this.config.get('KAFKA_GROUP_ID', { infer: true }),
      sessionTimeout: 45_000,
      heartbeatInterval: 10_000,
    });
    await consumer.connect();
    await consumer.subscribe({ topic: TREASURY_TOPIC, fromBeginning: false });
    this.consumer = consumer;

    await consumer.run({
      eachBatchAutoResolve: false,
      // Not destructured — kafkajs hands out plain function properties, not
      // `this`-bound methods, but ESLint's unbound-method rule can't tell
      // the difference and flags any destructured call site regardless.
      eachBatch: async (ctx) => {
        for (const message of ctx.batch.messages) {
          if (!ctx.isRunning() || ctx.isStale()) break;
          await this.handleOne(ctx.batch.topic, ctx.batch.partition, message);
          ctx.resolveOffset(message.offset);
          await ctx.heartbeat();
        }
        await ctx.commitOffsetsIfNecessary();
      },
    });
  }

  private offsetKey(topic: string, partition: number, message: KafkaMessage): string {
    return `${topic}/${partition}/${message.offset}`;
  }

  /// Records a message as dead-lettered and swallows any failure of that
  /// recording. If the dead-letter write itself fails the message is skipped
  /// anyway: it is unprocessable regardless of the DLQ's state, and blocking
  /// the partition on it turns the loss of one message into the loss of every
  /// message behind it. The `error`-level log is the only remaining trace, by
  /// design.
  private async deadLetter(
    topic: string,
    partition: number,
    message: KafkaMessage,
    error: Error,
  ): Promise<void> {
    this.transientAttempts.delete(this.offsetKey(topic, partition, message));
    try {
      await this.dlq.record(topic, partition, message, error);
      this.logger.warn(`Dead-lettered ${topic}/${partition}@${message.offset}: ${error.message}`);
    } catch (dlqError) {
      this.logger.error(
        `Dead-letter write FAILED for ${topic}/${partition}@${message.offset}; ` +
          `skipping message. Original: ${error.message}. DLQ: ${String(dlqError)}`,
      );
    }
  }

  private async handleOne(topic: string, partition: number, message: KafkaMessage): Promise<void> {
    if (message.value === null || message.value === undefined) {
      await this.deadLetter(topic, partition, message, new Error('message has no value'));
      return;
    }
    const value = message.value;

    let parsed;
    try {
      parsed = capacityEventEnvelope.parse(JSON.parse(value.toString('utf8')));
    } catch (err) {
      await this.deadLetter(topic, partition, message, err as Error);
      return;
    }

    try {
      await this.prisma.$transaction(
        async (tx) => {
          const claimed: number = await tx.$executeRaw`
          INSERT INTO processed_message (id, event_id, topic, partition, "offset", event_type, program_ref, payload_hash, schema_version, received_at)
          VALUES (gen_random_uuid(), ${parsed.event_id}, ${topic}, ${partition}, ${BigInt(message.offset)},
                  ${parsed.event_type}, ${parsed.program_ref}, ${createHash('sha256').update(value).digest('hex')}, ${parsed.schema_version}, now())
          ON CONFLICT (event_id) DO NOTHING
        `;
          if (claimed === 0) return;

          if (parsed.event_type === EVENT_TYPE.SNAPSHOT) {
            await this.enqueueReconciliationJob(tx, parsed);
          } else {
            await this.capacity.applyTreasuryDelta(tx, parsed);
          }
        },
        { timeout: 5_000 },
      );
      this.transientAttempts.delete(this.offsetKey(topic, partition, message));
    } catch (err) {
      await this.handleProcessingFailure(topic, partition, message, err, parsed.event_id);
    }
  }

  /// A permanent failure is dead-lettered immediately; a transient one is
  /// rethrown so kafkajs redelivers, but only up to a bound — an error
  /// misclassified as transient would otherwise block the head of the
  /// partition indefinitely.
  private async handleProcessingFailure(
    topic: string,
    partition: number,
    message: KafkaMessage,
    err: unknown,
    eventId: string,
  ): Promise<void> {
    const error = err instanceof Error ? err : new Error(String(err));

    if (!isTransientDbError(err)) {
      this.logger.error(`Permanent failure processing ${eventId}: ${error.message}`);
      await this.deadLetter(topic, partition, message, error);
      return;
    }

    const key = this.offsetKey(topic, partition, message);
    const attempts = (this.transientAttempts.get(key) ?? 0) + 1;
    this.transientAttempts.set(key, attempts);

    if (attempts >= MAX_TRANSIENT_ATTEMPTS) {
      this.logger.error(
        `Giving up on ${eventId} after ${attempts} transient failures: ${error.message}`,
      );
      await this.deadLetter(topic, partition, message, error);
      return;
    }

    this.logger.warn(
      `Transient failure processing ${eventId} (attempt ${attempts}/${MAX_TRANSIENT_ATTEMPTS}): ${error.message}`,
    );
    throw error; // let kafkajs redeliver
  }

  private async enqueueReconciliationJob(tx: Tx, event: SnapshotEvent): Promise<void> {
    const s = event.snapshot;
    await tx.reconciliationJob.upsert({
      where: { snapshotId: s.snapshot_id },
      create: {
        programRef: event.program_ref,
        snapshotId: s.snapshot_id,
        snapshotSeq: s.snapshot_seq,
        asOf: new Date(s.as_of),
        chunkCount: s.chunk_count,
        // Left empty on purpose: the statement below records this chunk's
        // index, and routing both the first and every later chunk through the
        // same append keeps one rule instead of two that can disagree.
        receivedChunks: [],
        positionCount: s.position_count,
        // Deliberately NOT a spread of `s` — s.positions carries raw bigint
        // local_ledger_seq fields that would break JSON serialisation, and
        // positions are already persisted separately below (the
        // reconciliation worker reads them from
        // reconciliation_snapshot_position, not from this header). Only the
        // scalar fields the worker's baseline+replay step actually needs.
        header: {
          event_id: event.event_id,
          snapshot_id: s.snapshot_id,
          snapshot_seq: s.snapshot_seq.toString(),
          as_of: s.as_of,
          total_limit: s.total_limit,
          reserved_amount: s.reserved_amount,
          acknowledged_local_seq: s.acknowledged_local_seq?.toString() ?? null,
          included_through_event_seq: s.included_through_event_seq?.toString() ?? null,
          program_currency: event.program_currency,
        },
      },
      update: {},
    });

    // Record the chunk index itself, and only if it is new. Counting messages
    // instead would let the same chunk redelivered `chunk_count` times satisfy
    // the completeness gate while the other chunks never arrived, applying a
    // snapshot whose positions are mostly missing.
    await tx.$executeRaw`
      UPDATE reconciliation_job
         SET received_chunks = CASE
               WHEN ${s.chunk_index}::int = ANY(received_chunks) THEN received_chunks
               ELSE array_append(received_chunks, ${s.chunk_index}::int)
             END
       WHERE snapshot_id = ${s.snapshot_id}`;
    if (s.positions.length > 0) {
      const job = await tx.reconciliationJob.findUniqueOrThrow({
        where: { snapshotId: s.snapshot_id },
      });
      await tx.reconciliationSnapshotPosition.createMany({
        data: s.positions.map((p) => ({
          jobId: job.id,
          invoiceRef: p.invoice_ref,
          localLedgerSeq: p.local_ledger_seq,
          reservedAmount: p.reserved_amount,
          currencyCode: p.currency,
          statusText: p.status,
          occurredAt: new Date(p.occurred_at),
        })),
        skipDuplicates: true,
      });
    }
  }
}
