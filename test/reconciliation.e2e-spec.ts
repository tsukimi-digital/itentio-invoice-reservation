import { INestApplication } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import request from 'supertest';
import type { App } from 'supertest/types';
import { Kafka, Producer } from 'kafkajs';
import { randomUUID } from 'node:crypto';
import { AppModule } from '../src/app.module';
import { configureApp } from '../src/bootstrap';
import { PrismaService } from '../src/prisma/prisma.service';
import { CapacityConsumerService } from '../src/kafka/capacity-consumer.service';
import { hashPassword } from '../src/auth/password';

const TOPIC = 'treasury.capacity-events';

jest.setTimeout(180_000);

async function waitFor<T>(
  probe: () => Promise<T | null | undefined>,
  what: string,
  timeoutMs = 45_000,
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const result = await probe();
    if (result !== null && result !== undefined) return result;
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
}

describe('Reconciliation (e2e)', () => {
  let app: INestApplication;
  let prisma: PrismaService;
  let producer: Producer;
  let adminToken: string;
  let readerToken: string;

  const email = `recon-${randomUUID()}@itentio.dev`;
  const readerEmail = `recon-reader-${randomUUID()}@itentio.dev`;
  const password = 'recon-test-password';

  async function publish(payload: unknown): Promise<void> {
    await producer.send({
      topic: TOPIC,
      messages: [{ key: 'recon', value: JSON.stringify(payload) }],
    });
  }

  function snapshotEvent(input: {
    programRef: string;
    reservedAmount: string;
    totalLimit: string;
    acknowledgedLocalSeq: string | null;
    includedThroughEventSeq: string | null;
    positions?: unknown[];
  }) {
    return {
      event_id: randomUUID(),
      event_type: 'program.capacity.snapshot',
      schema_version: 1,
      produced_at: new Date().toISOString(),
      program_ref: input.programRef,
      program_currency: 'GBP',
      snapshot: {
        snapshot_id: `SNAP-${randomUUID()}`,
        snapshot_seq: 1,
        as_of: new Date().toISOString(),
        total_limit: input.totalLimit,
        reserved_amount: input.reservedAmount,
        acknowledged_local_seq: input.acknowledgedLocalSeq,
        included_through_event_seq: input.includedThroughEventSeq,
        position_count: input.positions?.length ?? 0,
        chunk_index: 0,
        chunk_count: 1,
        positions: input.positions ?? [],
        positions_uri: null,
      },
    };
  }

  async function createProgram(prefix: string, totalLimit = '100000.00'): Promise<string> {
    const externalRef = `${prefix}-${randomUUID()}`;
    await prisma.program.create({
      data: { externalRef, name: prefix, currencyCode: 'GBP', totalLimit },
    });
    return externalRef;
  }

  beforeAll(async () => {
    const moduleFixture = await Test.createTestingModule({ imports: [AppModule] }).compile();
    app = moduleFixture.createNestApplication();
    configureApp(app);
    await app.init();
    prisma = app.get(PrismaService);

    await prisma.currency.upsert({
      where: { code: 'GBP' },
      update: {},
      create: { code: 'GBP', minorUnits: 2, name: 'Pound Sterling' },
    });
    await prisma.user.create({
      data: { email, passwordHash: hashPassword(password), displayName: 'Recon', role: 'ADMIN' },
    });
    await prisma.user.create({
      data: {
        email: readerEmail,
        passwordHash: hashPassword(password),
        displayName: 'Recon reader',
        role: 'READER',
      },
    });

    const kafka = new Kafka({
      clientId: 'e2e-recon-producer',
      brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(','),
    });
    producer = kafka.producer();
    await producer.connect();
    await app.get(CapacityConsumerService).ready;

    const login = await request(app.getHttpServer() as App)
      .post('/auth/login')
      .send({ email, password })
      .expect(200);
    adminToken = (login.body as { accessToken: string }).accessToken;

    const readerLogin = await request(app.getHttpServer() as App)
      .post('/auth/login')
      .send({ email: readerEmail, password })
      .expect(200);
    readerToken = (readerLogin.body as { accessToken: string }).accessToken;
  });

  afterAll(async () => {
    await producer?.disconnect();
    await app.close();
  });

  it('replays a treasury delta that arrived after the snapshot watermark', async () => {
    // The classic lost update on reconciliation. Before ADR-026 the replay
    // predicate was `treasury_event_seq > included_through_event_seq` while
    // nothing ever wrote treasury_event_seq — always NULL, `NULL > n` is NULL,
    // so the TREASURY branch never matched and the baseline silently erased
    // every delta the snapshot had not yet folded in.
    const programRef = await createProgram('PRG-RECON-DELTA');

    await publish({
      event_id: randomUUID(),
      event_type: 'program.capacity.delta',
      schema_version: 1,
      produced_at: new Date().toISOString(),
      program_ref: programRef,
      event_seq: 101,
      delta: { amount: '500.00', currency: 'GBP', direction: 'RESERVE' },
    });

    await waitFor(async () => {
      const p = await prisma.program.findUniqueOrThrow({ where: { externalRef: programRef } });
      return p.reservedAmount.toFixed(2) === '500.00' ? true : null;
    }, 'the delta to be applied');

    // Treasury reports 1000 and says it has folded in everything up to event
    // 100 — so our event 101 is NOT in that figure and must be replayed on top.
    await publish(
      snapshotEvent({
        programRef,
        reservedAmount: '1000.00',
        totalLimit: '100000.00',
        acknowledgedLocalSeq: '0',
        includedThroughEventSeq: '100',
      }),
    );

    const program = await waitFor(async () => {
      const p = await prisma.program.findUniqueOrThrow({ where: { externalRef: programRef } });
      return p.treasurySnapshotSeq !== null ? p : null;
    }, 'the baseline to be applied');

    // 1000 (treasury) + 500 (our unacknowledged delta) = 1500.
    // The old behaviour produced 1000, exposing 500 of capacity that was in
    // fact reserved — and `v_program_ledger_drift` could not detect it,
    // because the correcting ledger entry kept counter and ledger consistent
    // on the wrong number.
    expect(program.reservedAmount.toFixed(2)).toBe('1500.00');

    const baseline = await prisma.capacityLedgerEntry.findFirstOrThrow({
      where: { programId: program.id, entryType: 'RECONCILE_BASELINE' },
    });
    expect(baseline.metadata).toMatchObject({ replayStrategy: 'watermark', replayedCount: 1 });
  });

  it('falls back to a time window when the snapshot omits the ack watermark', async () => {
    // ADR-004/005 promised exactly this fallback and no code implemented it:
    // a missing watermark meant ackSeq = 0, i.e. replay every API entry ever
    // recorded, double-counting the whole history into a snapshot that
    // already contained it.
    const programRef = await createProgram('PRG-RECON-FALLBACK');

    await publish({
      event_id: randomUUID(),
      event_type: 'program.capacity.delta',
      schema_version: 1,
      // Deliberately old: outside the replay window, so the fallback must
      // treat it as already reflected in the snapshot.
      produced_at: new Date(Date.now() - 60 * 60 * 1000).toISOString(),
      program_ref: programRef,
      event_seq: null,
      delta: { amount: '300.00', currency: 'GBP', direction: 'RESERVE' },
    });

    await waitFor(async () => {
      const p = await prisma.program.findUniqueOrThrow({ where: { externalRef: programRef } });
      return p.reservedAmount.toFixed(2) === '300.00' ? true : null;
    }, 'the old delta to be applied');

    await publish(
      snapshotEvent({
        programRef,
        reservedAmount: '300.00',
        totalLimit: '100000.00',
        acknowledgedLocalSeq: null,
        includedThroughEventSeq: null,
      }),
    );

    const program = await waitFor(async () => {
      const p = await prisma.program.findUniqueOrThrow({ where: { externalRef: programRef } });
      return p.treasurySnapshotSeq !== null ? p : null;
    }, 'the baseline to be applied');

    // The delta occurred an hour before as_of, so it is treated as included:
    // 300 + 0 = 300, not 300 + 300.
    expect(program.reservedAmount.toFixed(2)).toBe('300.00');
    const baseline = await prisma.capacityLedgerEntry.findFirstOrThrow({
      where: { programId: program.id, entryType: 'RECONCILE_BASELINE' },
    });
    expect(baseline.metadata).toMatchObject({ replayStrategy: 'time-fallback' });
  });

  it('exposes recorded discrepancies to an ADMIN and hides them from a READER', async () => {
    const programRef = await createProgram('PRG-RECON-DISC');

    await publish(
      snapshotEvent({
        programRef,
        reservedAmount: '0.00',
        totalLimit: '100000.00',
        acknowledgedLocalSeq: '0',
        includedThroughEventSeq: '0',
        positions: [
          {
            invoice_ref: 'INV-TREASURY-ONLY',
            local_ledger_seq: null,
            reserved_amount: '42.00',
            currency: 'GBP',
            status: 'RESERVED',
            occurred_at: new Date().toISOString(),
          },
        ],
      }),
    );

    // ADR-011 claimed discrepancies were "visible and auditable"; nothing read
    // the table — no endpoint, no log, no alert.
    const body = await waitFor(async () => {
      const res = await request(app.getHttpServer() as App)
        .get(`/programs/${programRef}/discrepancies`)
        .set('Authorization', `Bearer ${adminToken}`)
        .expect(200);
      const payload = res.body as { discrepancies: { kind: string; invoiceRef: string }[] };
      return payload.discrepancies.length > 0 ? payload : null;
    }, 'the UNKNOWN_INVOICE discrepancy to surface');

    expect(body.discrepancies[0]).toMatchObject({
      kind: 'UNKNOWN_INVOICE',
      invoiceRef: 'INV-TREASURY-ONLY',
    });

    await request(app.getHttpServer() as App)
      .get(`/programs/${programRef}/discrepancies`)
      .set('Authorization', `Bearer ${readerToken}`)
      .expect(403);

    await request(app.getHttpServer() as App)
      .get(`/programs/${programRef}/discrepancies`)
      .expect(401);
  });

  it('publishes an outbox event for a release, not only for a reservation', async () => {
    // The acknowledgement watermark is "the highest local seq treasury has
    // folded in". That is only meaningful if every local seq is published —
    // and releases allocated a ledger seq while publishing nothing, so
    // treasury could acknowledge past a release it had never seen and
    // reconciliation would restore capacity that had in fact been returned.
    const programRef = await createProgram('PRG-RECON-OUTBOX', '1000.00');
    const invoiceRef = `INV-${randomUUID()}`;

    await request(app.getHttpServer() as App)
      .post(`/programs/${programRef}/reserve`)
      .set('Authorization', `Bearer ${adminToken}`)
      .set('Idempotency-Key', randomUUID())
      .send({
        invoiceRef,
        amount: '25.00',
        currency: 'GBP',
        requestedAt: new Date().toISOString(),
      })
      .expect(201);

    await request(app.getHttpServer() as App)
      .post(`/programs/${programRef}/release`)
      .set('Authorization', `Bearer ${adminToken}`)
      .set('Idempotency-Key', randomUUID())
      .send({ invoiceRef })
      .expect(201);

    const rows = await prisma.outboxMessage.findMany({ where: { key: programRef } });
    const types = rows.map((r) => (r.payload as { event_type: string }).event_type);
    expect(types).toContain('invoice.reserved');
    expect(types).toContain('invoice.released');

    // Both events carry a ledger seq, and they are consecutive — that
    // contiguity is exactly what makes the watermark sound.
    const seqs = rows
      .map((r) => Number((r.payload as { local_ledger_seq: string }).local_ledger_seq))
      .sort((a, b) => a - b);
    expect(seqs[1] - seqs[0]).toBe(1);
  });
});
