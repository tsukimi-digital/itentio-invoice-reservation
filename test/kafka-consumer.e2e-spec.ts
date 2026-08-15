import { INestApplication } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { Kafka, Producer } from 'kafkajs';
import { randomUUID } from 'node:crypto';
import { AppModule } from '../src/app.module';
import { configureApp } from '../src/bootstrap';
import { PrismaService } from '../src/prisma/prisma.service';
import { CapacityConsumerService } from '../src/kafka/capacity-consumer.service';

const TOPIC = 'treasury.capacity-events';

jest.setTimeout(120_000);

async function waitFor<T>(
  probe: () => Promise<T | null | undefined>,
  what: string,
  timeoutMs = 30_000,
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const result = await probe();
    if (result !== null && result !== undefined) return result;
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
}

/// Before this spec existed, nothing in the suite produced or consumed a
/// single Kafka message — the README claimed `test:e2e` exercised "real
/// Kafka", but the broker was only needed for the app to boot. Every defect in
/// the consumer, the dead-letter path and the inbox was invisible to CI.
describe('Kafka capacity consumer (e2e)', () => {
  let app: INestApplication;
  let prisma: PrismaService;
  let producer: Producer;

  const programRef = `PRG-KAFKA-${randomUUID()}`;

  function deltaEvent(overrides: Record<string, unknown> = {}) {
    return {
      event_id: randomUUID(),
      event_type: 'program.capacity.delta',
      schema_version: 1,
      produced_at: new Date().toISOString(),
      program_ref: programRef,
      event_seq: 1,
      delta: { amount: '100.00', currency: 'GBP', direction: 'RESERVE' },
      ...overrides,
    };
  }

  async function publish(value: string | Buffer | null): Promise<void> {
    await producer.send({ topic: TOPIC, messages: [{ key: programRef, value }] });
  }

  function reservedAmount(): Promise<string> {
    return prisma.program
      .findUniqueOrThrow({ where: { externalRef: programRef } })
      .then((p) => p.reservedAmount.toFixed(2));
  }

  /// Publishes a valid delta and waits for its ledger entry. Used as a probe:
  /// if this completes, the consumer has moved past whatever preceded it.
  async function assertConsumerStillAdvancing(amount = '1.00'): Promise<void> {
    const probe = deltaEvent({ delta: { amount, currency: 'GBP', direction: 'RESERVE' } });
    await publish(JSON.stringify(probe));
    await waitFor(
      () => prisma.capacityLedgerEntry.findUnique({ where: { eventId: probe.event_id } }),
      'the consumer to process a message queued behind the failing one',
    );
  }

  beforeAll(async () => {
    const moduleFixture = await Test.createTestingModule({ imports: [AppModule] }).compile();
    app = moduleFixture.createNestApplication();
    configureApp(app);
    await app.init();
    prisma = app.get(PrismaService);

    await prisma.currency.createMany({
      data: [
        { code: 'GBP', minorUnits: 2, name: 'Pound Sterling' },
        { code: 'USD', minorUnits: 2, name: 'US Dollar' },
      ],
      skipDuplicates: true,
    });
    await prisma.program.create({
      data: { externalRef: programRef, name: 'Kafka', currencyCode: 'GBP', totalLimit: '1000.00' },
    });

    const kafka = new Kafka({
      clientId: 'e2e-producer',
      brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(','),
    });
    producer = kafka.producer();
    await producer.connect();

    // Publish only once the consumer has joined its group: with
    // `fromBeginning: false` anything sent before the join can be skipped.
    await app.get(CapacityConsumerService).ready;
  });

  afterAll(async () => {
    await producer?.disconnect();
    await app.close();
  });

  it('applies a well-formed treasury delta', async () => {
    await publish(JSON.stringify(deltaEvent()));

    await waitFor(
      async () => ((await reservedAmount()) === '100.00' ? true : null),
      'the delta to be applied',
    );
  });

  it('records the treasury sequence on the ledger entry', async () => {
    const event = deltaEvent({
      event_seq: 42,
      delta: { amount: '10.00', currency: 'GBP', direction: 'RESERVE' },
    });
    await publish(JSON.stringify(event));

    // treasury_event_seq was written by nothing at all, which left the
    // TREASURY branch of reconciliation's replay permanently unsatisfiable —
    // `NULL > n` is NULL — so baselines silently erased treasury deltas.
    const entry = await waitFor(
      () => prisma.capacityLedgerEntry.findUnique({ where: { eventId: event.event_id } }),
      'the treasury ledger entry',
    );
    expect(entry.treasuryEventSeq).toBe(42n);
    expect(entry.origin).toBe('TREASURY');
  });

  it('applies a duplicated event_id exactly once', async () => {
    const before = await reservedAmount();
    const event = deltaEvent({ delta: { amount: '5.00', currency: 'GBP', direction: 'RESERVE' } });

    await publish(JSON.stringify(event));
    await waitFor(
      () => prisma.processedMessage.findUnique({ where: { eventId: event.event_id } }),
      'the inbox row',
    );

    // Same event_id, republished — at-least-once delivery made visible.
    await publish(JSON.stringify(event));
    await assertConsumerStillAdvancing('0.01');

    expect(await reservedAmount()).toBe((Number(before) + 5 + 0.01).toFixed(2));
    expect(await prisma.capacityLedgerEntry.count({ where: { eventId: event.event_id } })).toBe(1);
  });

  it('dead-letters an unparseable message instead of blocking the partition', async () => {
    // The dead-letter writer used to run JSON.parse on the very payload that
    // had just failed to parse. It threw inside the DLQ path, the exception
    // escaped eachBatch, kafkajs retried the batch forever and the offset
    // never advanced — one byte of rubbish stopped all consumption, and there
    // was not even a dead-letter row to show for it.
    await publish(Buffer.from(`not-json-${randomUUID()}`, 'utf8'));

    await waitFor(async () => {
      const row = await prisma.deadLetterMessage.findFirst({
        where: { topic: TOPIC },
        orderBy: { createdAt: 'desc' },
      });
      const payload = row?.payload as { __unparsed?: boolean; reason?: string } | null;
      return payload?.__unparsed === true ? row : null;
    }, 'a dead-letter row for the unparseable message');

    await assertConsumerStillAdvancing();
  });

  it('dead-letters a delta whose currency contradicts the programme', async () => {
    // The programme is GBP. A USD amount added 1:1 to reserved_amount used to
    // be booked without comment — the schema validated delta.currency and the
    // handler then never read it.
    const before = await reservedAmount();
    const dlqBefore = await prisma.deadLetterMessage.count({ where: { topic: TOPIC } });

    await publish(
      JSON.stringify(
        deltaEvent({ delta: { amount: '500.00', currency: 'USD', direction: 'RESERVE' } }),
      ),
    );

    await waitFor(async () => {
      const count = await prisma.deadLetterMessage.count({ where: { topic: TOPIC } });
      return count > dlqBefore ? count : null;
    }, 'the mismatched-currency delta to be dead-lettered');

    await assertConsumerStillAdvancing();
    expect(await reservedAmount()).toBe((Number(before) + 1).toFixed(2));
  });

  it('dead-letters a delta that would breach the programme limit', async () => {
    // Previously this hit the program_no_overcommit CHECK, was classified as
    // "transient" and retried identically forever.
    const before = await reservedAmount();

    await publish(
      JSON.stringify(
        deltaEvent({ delta: { amount: '999999.00', currency: 'GBP', direction: 'RESERVE' } }),
      ),
    );

    await assertConsumerStillAdvancing();
    expect(await reservedAmount()).toBe((Number(before) + 1).toFixed(2));
  });
});
