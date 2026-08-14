import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Kafka, Producer } from 'kafkajs';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../prisma/prisma.service';
import type { Env } from '../config/env.schema';

/// Polls PENDING outbox rows and publishes them. A separate relay (rather
/// than publishing inline in the HTTP request) keeps Kafka availability off
/// the reservation's critical path — a broker outage delays acknowledgement
/// to treasury, never blocks a reservation.
@Injectable()
export class OutboxRelayService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(OutboxRelayService.name);
  private producer!: Producer;
  private timer?: NodeJS.Timeout;

  constructor(
    private readonly prisma: PrismaService,
    private readonly config: ConfigService<Env, true>,
  ) {}

  async onModuleInit(): Promise<void> {
    const kafka = new Kafka({
      clientId: this.config.get('KAFKA_CLIENT_ID', { infer: true }),
      brokers: this.config.get('KAFKA_BROKERS', { infer: true }),
    });
    this.producer = kafka.producer({ idempotent: true });
    await this.producer.connect();
    this.timer = setInterval(() => void this.relayBatch(), 1_000);
  }

  async onModuleDestroy(): Promise<void> {
    if (this.timer) clearInterval(this.timer);
    await this.producer?.disconnect();
  }

  private async relayBatch(): Promise<void> {
    const pending = await this.prisma.outboxMessage.findMany({
      where: { status: 'PENDING' },
      take: 50,
      orderBy: { createdAt: 'asc' },
    });
    for (const msg of pending) {
      try {
        const payload = msg.payload as { event_id: string };
        await this.producer.send({
          topic: msg.topic,
          messages: [
            {
              key: msg.key,
              value: JSON.stringify(msg.payload),
              headers: { event_id: payload.event_id },
            },
          ],
        });
        await this.prisma.outboxMessage.update({
          where: { id: msg.id },
          data: { status: 'SENT', sentAt: new Date() },
        });
      } catch (err) {
        this.logger.warn(`Outbox relay failed for ${msg.id}: ${(err as Error).message}`);
        await this.prisma.outboxMessage.update({
          where: { id: msg.id },
          data: { attempts: { increment: 1 }, lastError: (err as Error).message },
        });
      }
    }
  }
}
