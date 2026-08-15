import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Kafka, Producer } from 'kafkajs';
import { ConfigService } from '@nestjs/config';
import { Prisma } from '@prisma/client';
import { PrismaService } from '../prisma/prisma.service';
import type { Env } from '../config/env.schema';

const POLL_INTERVAL_MS = 1_000;
const BATCH_SIZE = 50;
/// After this many failed publish attempts a row is parked in FAILED rather
/// than retried forever. `attempts` was previously incremented and never read,
/// and the FAILED enum value existed but was never written — so a permanently
/// unpublishable row was retried once a second indefinitely, and (because the
/// batch is ordered by created_at and capped at 50) permanently starved every
/// newer message behind it.
const MAX_ATTEMPTS = 8;

interface ClaimedMessage {
  id: string;
  topic: string;
  key: string;
  payload: Prisma.JsonValue;
  attempts: number;
}

/// Polls PENDING outbox rows and publishes them. A separate relay (rather
/// than publishing inline in the HTTP request) keeps Kafka availability off
/// the reservation's critical path — a broker outage delays acknowledgement
/// to treasury, never blocks a reservation.
@Injectable()
export class OutboxRelayService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(OutboxRelayService.name);
  private producer!: Producer;
  private timer?: NodeJS.Timeout;
  private running = false;
  private stopping = false;
  private inFlight: Promise<void> = Promise.resolve();

  constructor(
    private readonly prisma: PrismaService,
    private readonly config: ConfigService<Env, true>,
  ) {}

  onModuleInit(): void {
    const kafka = new Kafka({
      clientId: this.config.get('KAFKA_CLIENT_ID', { infer: true }),
      brokers: this.config.get('KAFKA_BROKERS', { infer: true }),
    });
    this.producer = kafka.producer({ idempotent: true });

    // Connect lazily and never at boot: an unreachable broker used to reject
    // onModuleInit, which aborts NestFactory.create and takes the whole HTTP
    // API down with it — even though reserve/release/availability touch only
    // Postgres. Publication is meant to be the deferrable part.
    this.producer.connect().catch((err: unknown) => {
      this.logger.warn(
        `Producer connect failed, will retry on the next relay tick: ${String(err)}`,
      );
    });

    this.timer = setInterval(() => {
      void this.tick();
    }, POLL_INTERVAL_MS);
  }

  async onModuleDestroy(): Promise<void> {
    this.stopping = true;
    if (this.timer) clearInterval(this.timer);
    // Let an in-flight batch finish before tearing the producer down,
    // otherwise it publishes into a disconnected client and records failures
    // against messages that may in fact have been delivered.
    await this.inFlight;
    await this.producer?.disconnect().catch(() => undefined);
  }

  /// Re-entrancy guard: setInterval fires on a fixed schedule regardless of
  /// whether the previous run finished. A batch slower than the interval used
  /// to overlap with itself and publish the same rows twice from a single
  /// instance.
  private async tick(): Promise<void> {
    if (this.running || this.stopping) return;
    this.running = true;
    this.inFlight = this.relayBatch()
      .catch((err: unknown) => {
        // A throw here would otherwise be an unhandled rejection, since the
        // interval callback cannot await.
        this.logger.error(`Outbox relay tick failed: ${String(err)}`);
      })
      .finally(() => {
        this.running = false;
      });
    await this.inFlight;
  }

  private async relayBatch(): Promise<void> {
    const claimed = await this.claimBatch();
    if (claimed.length === 0) return;

    // Group by partition key (the programme) and publish each group in order,
    // abandoning the rest of a group after its first failure. Messages carry
    // the ledger seq treasury uses as an acknowledgement watermark, so
    // publishing seq N+1 after seq N failed would let treasury acknowledge
    // past a message it never received.
    const byKey = new Map<string, ClaimedMessage[]>();
    for (const message of claimed) {
      const group = byKey.get(message.key);
      if (group) group.push(message);
      else byKey.set(message.key, [message]);
    }

    await Promise.all([...byKey.values()].map((group) => this.publishGroup(group)));
  }

  private async publishGroup(group: ClaimedMessage[]): Promise<void> {
    for (const message of group) {
      const published = await this.publish(message);
      if (!published) return; // preserve per-key ordering
    }
  }

  private async publish(message: ClaimedMessage): Promise<boolean> {
    try {
      const payload = message.payload as { event_id?: string } | null;
      await this.producer.send({
        topic: message.topic,
        messages: [
          {
            key: message.key,
            value: JSON.stringify(message.payload),
            headers: payload?.event_id ? { event_id: payload.event_id } : undefined,
          },
        ],
      });
      // Guarded on PENDING so a concurrent claim cannot resurrect a row that
      // another path already settled.
      await this.prisma.outboxMessage.updateMany({
        where: { id: message.id, status: 'PENDING' },
        data: { status: 'SENT', sentAt: new Date() },
      });
      return true;
    } catch (err) {
      const reason = err instanceof Error ? err.message : String(err);
      const exhausted = message.attempts >= MAX_ATTEMPTS;
      await this.prisma.outboxMessage.updateMany({
        where: { id: message.id, status: 'PENDING' },
        data: exhausted ? { status: 'FAILED', lastError: reason } : { lastError: reason },
      });
      this.logger.warn(
        exhausted
          ? `Outbox message ${message.id} parked as FAILED after ${message.attempts} attempts: ${reason}`
          : `Outbox relay failed for ${message.id} (attempt ${message.attempts}): ${reason}`,
      );
      return false;
    }
  }

  /// Claims a batch atomically: SKIP LOCKED so two instances never take the
  /// same rows, `attempts` incremented and `available_at` pushed forward in
  /// the same statement so a crash mid-publish still counts as an attempt and
  /// the row backs off instead of spinning.
  private claimBatch(): Promise<ClaimedMessage[]> {
    return this.prisma.$queryRaw<ClaimedMessage[]>`
      UPDATE outbox_message m
         SET attempts = m.attempts + 1,
             available_at = now() + make_interval(secs => least(power(2, m.attempts)::int, 300))
       WHERE m.id IN (
         SELECT id FROM outbox_message
          WHERE status = 'PENDING' AND available_at <= now()
          ORDER BY created_at
          LIMIT ${BATCH_SIZE}
          FOR UPDATE SKIP LOCKED
       )
      RETURNING m.id, m.topic, m.key, m.payload, m.attempts
    `;
  }
}
