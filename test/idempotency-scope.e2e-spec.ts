import { INestApplication } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import request from 'supertest';
import type { App } from 'supertest/types';
import { randomUUID } from 'node:crypto';
import { AppModule } from '../src/app.module';
import { configureApp } from '../src/bootstrap';
import { PrismaService } from '../src/prisma/prisma.service';
import { hashPassword } from '../src/auth/password';

/// The idempotency fingerprint covers the method and concrete URL, not the
/// request BODY alone. `programRef` is a path parameter, so two reservations
/// against different programmes carry byte-identical bodies: on a body-only
/// fingerprint the second call replays the first programme's response
/// (HTTP 201, wrong invoiceId) and the second programme is never debited,
/// with the caller none the wiser.
describe('Idempotency-Key scope (e2e)', () => {
  let app: INestApplication;
  let prisma: PrismaService;
  let token: string;

  const programA = `PRG-IDEM-A-${randomUUID()}`;
  const programB = `PRG-IDEM-B-${randomUUID()}`;
  const email = `idem-${randomUUID()}@itentio.dev`;
  const password = 'idem-test-password';

  const auth = () => ({ Authorization: `Bearer ${token}` });

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
    for (const externalRef of [programA, programB]) {
      await prisma.program.create({
        data: { externalRef, name: externalRef, currencyCode: 'GBP', totalLimit: '1000.00' },
      });
    }
    await prisma.user.create({
      data: {
        email,
        passwordHash: hashPassword(password),
        displayName: 'Idempotency tester',
        role: 'ADMIN',
      },
    });

    const login = await request(app.getHttpServer() as App)
      .post('/auth/login')
      .send({ email, password })
      .expect(200);
    token = (login.body as { accessToken: string }).accessToken;
  });

  afterAll(async () => {
    await app.close();
  });

  function reserve(programRef: string, key: string, body: Record<string, unknown>) {
    return request(app.getHttpServer() as App)
      .post(`/programs/${programRef}/reserve`)
      .set(auth())
      .set('Idempotency-Key', key)
      .send(body);
  }

  it('does not replay one programme’s response for a request against another', async () => {
    const key = randomUUID();
    const body = {
      invoiceRef: `INV-${randomUUID()}`,
      amount: '100.00',
      currency: 'GBP',
      requestedAt: new Date().toISOString(),
    };

    const first = await reserve(programA, key, body).expect(201);

    // Same key, same body, different programme: a conflict rather than a 201
    // replaying programme A's payload, because the fingerprint covers the
    // method and the concrete URL.
    const second = await reserve(programB, key, body).expect(409);
    expect(second.body).not.toEqual(first.body);

    const b = await prisma.program.findUniqueOrThrow({ where: { externalRef: programB } });
    expect(b.reservedAmount.toFixed(2)).toBe('0.00');
  });

  it('still replays a genuine repeat of the same call', async () => {
    const key = randomUUID();
    const body = {
      invoiceRef: `INV-${randomUUID()}`,
      amount: '50.00',
      currency: 'GBP',
      requestedAt: new Date().toISOString(),
    };

    const first = await reserve(programA, key, body).expect(201);
    const replay = await reserve(programA, key, body).expect(201);

    expect(replay.body).toEqual(first.body);

    // Exactly one debit, not two.
    const entries = await prisma.capacityLedgerEntry.count({
      where: { invoice: { externalRef: body.invoiceRef }, entryType: 'RESERVE' },
    });
    expect(entries).toBe(1);
  });

  it('rejects the same key reused with a different body', async () => {
    const key = randomUUID();
    const base = {
      invoiceRef: `INV-${randomUUID()}`,
      currency: 'GBP',
      requestedAt: new Date().toISOString(),
    };

    await reserve(programA, key, { ...base, amount: '10.00' }).expect(201);
    await reserve(programA, key, { ...base, amount: '20.00' }).expect(409);
  });

  it('requires the Idempotency-Key header on a mutating call', async () => {
    await request(app.getHttpServer() as App)
      .post(`/programs/${programA}/reserve`)
      .set(auth())
      .send({
        invoiceRef: `INV-${randomUUID()}`,
        amount: '10.00',
        currency: 'GBP',
        requestedAt: new Date().toISOString(),
      })
      .expect(400);
  });
});
