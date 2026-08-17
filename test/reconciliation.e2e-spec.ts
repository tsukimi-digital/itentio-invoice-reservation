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
    snapshotId?: string;
    chunkIndex?: number;
    chunkCount?: number;
  }) {
    return {
      event_id: randomUUID(),
      event_type: 'program.capacity.snapshot',
      schema_version: 1,
      produced_at: new Date().toISOString(),
      program_ref: input.programRef,
      program_currency: 'GBP',
      snapshot: {
        snapshot_id: input.snapshotId ?? `SNAP-${randomUUID()}`,
        snapshot_seq: 1,
        as_of: new Date().toISOString(),
        total_limit: input.totalLimit,
        reserved_amount: input.reservedAmount,
        acknowledged_local_seq: input.acknowledgedLocalSeq,
        included_through_event_seq: input.includedThroughEventSeq,
        position_count: input.positions?.length ?? 0,
        chunk_index: input.chunkIndex ?? 0,
        chunk_count: input.chunkCount ?? 1,
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
    // Pins the guard against the classic lost update on reconciliation
    // (ADR-06). The replay predicate is
    // `treasury_event_seq > included_through_event_seq`, so that column must
    // be populated: NULL makes the TREASURY branch unsatisfiable — `NULL > n`
    // is NULL — and the baseline erases every delta not yet folded in.
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
    // A bare 1000 would expose 500 of capacity that is in fact reserved, and
    // `v_program_ledger_drift` cannot detect that, because the correcting
    // ledger entry keeps counter and ledger consistent on the wrong number.
    expect(program.reservedAmount.toFixed(2)).toBe('1500.00');

    const baseline = await prisma.capacityLedgerEntry.findFirstOrThrow({
      where: { programId: program.id, entryType: 'RECONCILE_BASELINE' },
    });
    expect(baseline.metadata).toMatchObject({ replayStrategy: 'watermark', replayedCount: 1 });
  });

  it('falls back to a time window when the snapshot omits the ack watermark', async () => {
    // ADR-06's fallback. A missing watermark must not be read as ackSeq = 0,
    // which replays every API entry ever recorded and double-counts the whole
    // history into a snapshot that already contains it.
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

    // Pins that a recorded discrepancy is actually reachable by an operator
    // over HTTP — "visible and auditable" needs a reader (ADR-07).
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

  it('holds back a multi-chunk snapshot when one chunk is merely redelivered', async () => {
    // Delivery is at-least-once, so the same chunk arrives more than once as a
    // matter of course. Completeness is therefore the set of chunk indexes
    // seen, never a count of messages: counting would let three copies of
    // chunk 0 satisfy a three-chunk snapshot, and the positions carried by the
    // chunks that never arrived would be silently absent from reconciliation
    // while the snapshot was marked done and its sequence advanced.
    const programRef = await createProgram('PRG-RECON-CHUNK', '1000.00');
    const snapshotId = `SNAP-${randomUUID()}`;

    for (let i = 0; i < 3; i += 1) {
      await publish(
        snapshotEvent({
          programRef,
          totalLimit: '4242.00',
          reservedAmount: '42.00',
          acknowledgedLocalSeq: null,
          includedThroughEventSeq: null,
          snapshotId,
          chunkIndex: 0,
          chunkCount: 3,
        }),
      );
    }

    // Wait out several worker ticks: this asserts that nothing happens, so it
    // has to give the worker every chance to act before concluding it did not.
    await new Promise((resolve) => setTimeout(resolve, 8_000));

    const job = await prisma.reconciliationJob.findUniqueOrThrow({ where: { snapshotId } });
    expect(job.receivedChunks).toEqual([0]);
    expect(job.status).toBe('PENDING');

    const program = await prisma.program.findUniqueOrThrow({ where: { externalRef: programRef } });
    expect(program.totalLimit.toFixed(2)).toBe('1000.00');
    expect(program.reservedAmount.toFixed(2)).toBe('0.00');
  });

  it('applies a multi-chunk snapshot once every distinct chunk has arrived', async () => {
    const programRef = await createProgram('PRG-RECON-CHUNK-OK', '1000.00');
    const snapshotId = `SNAP-${randomUUID()}`;

    // Chunk 1 is sent twice to prove a duplicate neither blocks completion nor
    // stands in for a chunk that is still missing.
    for (const chunkIndex of [0, 1, 1, 2]) {
      await publish(
        snapshotEvent({
          programRef,
          totalLimit: '4242.00',
          reservedAmount: '42.00',
          acknowledgedLocalSeq: null,
          includedThroughEventSeq: null,
          snapshotId,
          chunkIndex,
          chunkCount: 3,
        }),
      );
    }

    const program = await waitFor(async () => {
      const row = await prisma.program.findUniqueOrThrow({ where: { externalRef: programRef } });
      return row.totalLimit.equals(4242) ? row : null;
    }, 'the complete multi-chunk snapshot to be applied');

    expect(program.totalLimit.toFixed(2)).toBe('4242.00');
    expect(program.reservedAmount.toFixed(2)).toBe('42.00');

    const job = await prisma.reconciliationJob.findUniqueOrThrow({ where: { snapshotId } });
    expect([...job.receivedChunks].sort((a, b) => a - b)).toEqual([0, 1, 2]);
  });

  it('publishes an outbox event for a release, not only for a reservation', async () => {
    // The acknowledgement watermark is "the highest local seq treasury has
    // folded in", which is only meaningful if every local seq is published.
    // A release that allocates a ledger seq but publishes nothing lets
    // treasury acknowledge past a release it never saw, and reconciliation
    // then restores capacity that had in fact been returned.
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
