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

/// Exercises the consumer against a real broker: a message is produced,
/// consumed, and its effect on the inbox, the ledger and the dead-letter path
/// asserted. Booting the app against Kafka proves nothing on its own — defects
/// in the consumer are only observable with traffic actually flowing.
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

    // Pins that treasury_event_seq is populated. Reconciliation's TREASURY
    // replay predicate is `treasury_event_seq > included_through_event_seq`,
    // and `NULL > n` is NULL, so a missing seq lets a baseline erase the delta.
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
    // Pins that a malformed payload is stored rather than crashing the writer:
    // the dead-letter path must never itself parse the body it is quarantining.
    // If it throws, the exception escapes eachBatch, kafkajs retries the batch
    // forever and the offset never advances — one byte of rubbish stops all
    // consumption, with not even a dead-letter row to show for it.
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
    // The programme is GBP. delta.currency must be enforced by the handler and
    // not merely validated by the schema: a USD amount added 1:1 to
    // reserved_amount books capacity at a figure nobody computed.
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
    // A program_no_overcommit CHECK breach is permanent, not transient: the
    // identical delta can only fail again, so it must be dead-lettered.
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
