import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Consumer, Kafka, KafkaMessage } from 'kafkajs';
import { createHash } from 'node:crypto';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../prisma/prisma.service';
import { CapacityService } from '../capacity/capacity.service';
import { capacityEventEnvelope, SnapshotEvent } from './schemas/capacity-event.schema';
import { DeadLetterService } from './dead-letter.service';
import type { Env } from '../config/env.schema';
import type { Tx } from '../idempotency/idempotency';

const TREASURY_TOPIC = 'treasury.capacity-events';

/// At-least-once consumption + idempotent DB writes, never Kafka EOS — see
/// docs/DECISIONS.md ADR-007/008. A bulk snapshot is never applied inline
/// here; it's parked as a ReconciliationJob and applied by a separate
/// worker in bounded chunks, so a long reconciliation never risks a
/// consumer rebalance (kafkajs has no max.poll.interval.ms — heartbeat()
/// between every message is what keeps this consumer alive under load).
@Injectable()
export class CapacityConsumerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(CapacityConsumerService.name);
  private consumer!: Consumer;

  constructor(
    private readonly prisma: PrismaService,
    private readonly capacity: CapacityService,
    private readonly dlq: DeadLetterService,
    private readonly config: ConfigService<Env, true>,
  ) {}

  async onModuleInit(): Promise<void> {
    const kafka = new Kafka({
      clientId: this.config.get('KAFKA_CLIENT_ID', { infer: true }),
      brokers: this.config.get('KAFKA_BROKERS', { infer: true }),
    });

    // A consumer subscribing to a topic nothing has ever produced to races
    // the broker's auto-create and throws an uncaught (though "retriable")
    // UNKNOWN_TOPIC_OR_PARTITION. In a real deployment the treasury system
    // provisions this topic out-of-band; locally, nothing does — so this
    // service self-provisions it defensively. createTopics() is idempotent
    // (a no-op if the topic already exists), so this is always safe to run.
    const admin = kafka.admin();
    await admin.connect();
    try {
      await admin.createTopics({ topics: [{ topic: TREASURY_TOPIC, numPartitions: 1 }] });
    } catch (err) {
      this.logger.warn(`Could not pre-create ${TREASURY_TOPIC}: ${(err as Error).message}`);
    } finally {
      await admin.disconnect();
    }

    this.consumer = kafka.consumer({
      groupId: this.config.get('KAFKA_GROUP_ID', { infer: true }),
      sessionTimeout: 45_000,
      heartbeatInterval: 10_000,
    });
    await this.consumer.connect();
    await this.consumer.subscribe({ topic: TREASURY_TOPIC, fromBeginning: false });

    await this.consumer.run({
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

  async onModuleDestroy(): Promise<void> {
    await this.consumer?.disconnect();
  }

  private async handleOne(topic: string, partition: number, message: KafkaMessage): Promise<void> {
    const eventId = message.headers?.event_id?.toString() ?? '';
    let parsed;
    try {
      parsed = capacityEventEnvelope.parse(JSON.parse(message.value!.toString()));
    } catch (err) {
      await this.dlq.record(topic, partition, message, err as Error);
      return;
    }

    try {
      await this.prisma.$transaction(
        async (tx) => {
          const claimed: number = await tx.$executeRaw`
          INSERT INTO processed_message (id, event_id, topic, partition, "offset", event_type, program_ref, payload_hash, schema_version, received_at)
          VALUES (gen_random_uuid(), ${parsed.event_id}, ${topic}, ${partition}, ${BigInt(message.offset)},
                  ${parsed.event_type}, ${parsed.program_ref}, ${createHash('sha256').update(message.value!).digest('hex')}, ${parsed.schema_version}, now())
          ON CONFLICT (event_id) DO NOTHING
        `;
          if (claimed === 0) return;

          if (parsed.event_type === 'program.capacity.snapshot') {
            await this.enqueueReconciliationJob(tx, parsed);
          } else {
            await this.capacity.applyTreasuryDelta(tx, parsed);
          }
        },
        { timeout: 5_000 },
      );
    } catch (err) {
      this.logger.error(`Transient failure processing ${eventId}: ${(err as Error).message}`);
      throw err; // let kafkajs retry; do not DLQ transient DB errors
    }
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
        chunksReceived: 1,
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
      update: { chunksReceived: { increment: 1 } },
    });
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
