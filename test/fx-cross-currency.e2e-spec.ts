import { INestApplication } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import request from 'supertest';
import type { App } from 'supertest/types';
import { randomUUID } from 'node:crypto';
import { AppModule } from '../src/app.module';
import { configureApp } from '../src/bootstrap';
import { PrismaService } from '../src/prisma/prisma.service';
import { hashPassword } from '../src/auth/password';

/// The assignment's one explicitly highlighted requirement — "Programs and
/// invoices may be denominated in different currencies" — had no end-to-end
/// coverage at all: every spec used GBP for both sides, and DbFxRateProvider
/// was never executed by any test (the concurrency spec deliberately injects a
/// provider that throws). That gap is why the quantisation defect in ADR-020
/// survived: with GBP -> GBP it is invisible.
describe('Cross-currency reservation (e2e)', () => {
  let app: INestApplication;
  let prisma: PrismaService;
  let token: string;

  const programRef = `PRG-JPY-${randomUUID()}`;
  const TOTAL_LIMIT = '10000000';
  const email = `fx-${randomUUID()}@itentio.dev`;
  const password = 'fx-test-password';

  const auth = () => ({ Authorization: `Bearer ${token}` });

  beforeAll(async () => {
    const moduleFixture = await Test.createTestingModule({ imports: [AppModule] }).compile();
    app = moduleFixture.createNestApplication();
    configureApp(app);
    await app.init();
    prisma = app.get(PrismaService);

    await prisma.currency.createMany({
      data: [
        { code: 'GBP', minorUnits: 2, name: 'Pound Sterling' },
        { code: 'JPY', minorUnits: 0, name: 'Japanese Yen' },
        { code: 'CHF', minorUnits: 2, name: 'Swiss Franc' },
      ],
      skipDuplicates: true,
    });

    // A fresh rate, so the run does not depend on how long ago the database
    // was seeded (rates now expire — see FX_MAX_RATE_AGE_HOURS).
    // Canonical direction is amount_in_quote = amount_in_base * rate, so this
    // row says 1 JPY = 0.0052 GBP; GBP -> JPY resolves it inverted and divides.
    await prisma.fxRate.create({
      data: {
        baseCurrencyCode: 'JPY',
        quoteCurrencyCode: 'GBP',
        rate: '0.005200000000',
        source: 'SEED',
        asOf: new Date(),
      },
    });

    // A deliberately ancient rate for a pair with no fresh alternative.
    // createMany/skipDuplicates because the fixed asOf makes this row unique
    // across runs — a plain create fails the second time the spec runs.
    await prisma.fxRate.createMany({
      data: [
        {
          baseCurrencyCode: 'CHF',
          quoteCurrencyCode: 'JPY',
          rate: '170.000000000000',
          source: 'SEED',
          asOf: new Date('2019-01-01T00:00:00Z'),
        },
      ],
      skipDuplicates: true,
    });

    await prisma.program.create({
      data: {
        externalRef: programRef,
        name: 'JPY programme',
        currencyCode: 'JPY',
        totalLimit: TOTAL_LIMIT,
      },
    });

    await prisma.user.create({
      data: {
        email,
        passwordHash: hashPassword(password),
        displayName: 'FX tester',
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

  function availability() {
    return request(app.getHttpServer() as App)
      .get(`/programs/${programRef}`)
      .set(auth())
      .expect(200);
  }

  it('reserves the full invoice amount without rounding it to the programme scale', async () => {
    const invoiceRef = `INV-${randomUUID()}`;

    // 1234.56 GBP / 0.0052 = 237415.3846... -> 237415 JPY (HALF_EVEN, 0 dp).
    // Quantising the face amount at the PROGRAMME's scale first gave
    // Money.of('1234.56','GBP',0) = 1235, hence 1235 / 0.0052 = 237500 JPY:
    // 85 JPY of capacity consumed that nobody asked for, on every invoice.
    const res = await request(app.getHttpServer() as App)
      .post(`/programs/${programRef}/reserve`)
      .set(auth())
      .set('Idempotency-Key', randomUUID())
      .send({
        invoiceRef,
        amount: '1234.56',
        currency: 'GBP',
        requestedAt: new Date().toISOString(),
      })
      .expect(201);

    expect((res.body as { reservedAmount: string }).reservedAmount).toBe('237415');

    // The invoice's own record of its face value must be the amount that was
    // actually requested, not a rounded one — otherwise the audit trail lies.
    const invoice = await prisma.invoice.findFirstOrThrow({ where: { externalRef: invoiceRef } });
    expect(invoice.faceAmount.toFixed(2)).toBe('1234.56');
    expect(invoice.currencyCode).toBe('GBP');
    expect(invoice.reservedProgramAmount?.toFixed(0)).toBe('237415');
  });

  it('returns availability to exactly its prior value after release', async () => {
    const before = await availability();
    const invoiceRef = `INV-${randomUUID()}`;

    await request(app.getHttpServer() as App)
      .post(`/programs/${programRef}/reserve`)
      .set(auth())
      .set('Idempotency-Key', randomUUID())
      .send({
        invoiceRef,
        amount: '987.65',
        currency: 'GBP',
        requestedAt: new Date().toISOString(),
      })
      .expect(201);

    const during = await availability();
    expect((during.body as { available: string }).available).not.toBe(
      (before.body as { available: string }).available,
    );

    await request(app.getHttpServer() as App)
      .post(`/programs/${programRef}/release`)
      .set(auth())
      .set('Idempotency-Key', randomUUID())
      .send({ invoiceRef })
      .expect(201);

    const after = await availability();
    // Zero drift by construction: release replays the frozen amount rather
    // than reconverting at a new rate (ADR-003).
    expect((after.body as { available: string }).available).toBe(
      (before.body as { available: string }).available,
    );
  });

  it('rejects an amount carrying more precision than the invoice currency allows', async () => {
    // JPY has no minor units. 1000.5 JPY is malformed input, not a rounding
    // opportunity — and it used to reach a CHECK constraint and surface as 500.
    await request(app.getHttpServer() as App)
      .post(`/programs/${programRef}/reserve`)
      .set(auth())
      .set('Idempotency-Key', randomUUID())
      .send({
        invoiceRef: `INV-${randomUUID()}`,
        amount: '1000.5',
        currency: 'JPY',
        requestedAt: new Date().toISOString(),
      })
      .expect(400);
  });

  it('refuses a stale rate rather than pricing against it silently', async () => {
    // The only CHF -> JPY rate on file is from 2019. `valid_until` existed in
    // the schema and was read by nothing, and there was no maximum age at all,
    // so a rate of any vintage priced a reservation without comment.
    const res = await request(app.getHttpServer() as App)
      .post(`/programs/${programRef}/reserve`)
      .set(auth())
      .set('Idempotency-Key', randomUUID())
      .send({
        invoiceRef: `INV-${randomUUID()}`,
        amount: '100.00',
        currency: 'CHF',
        requestedAt: new Date().toISOString(),
      })
      .expect(422);

    expect((res.body as { error: string }).error).toBe('FX_RATE_UNAVAILABLE');
  });

  describe('hostile input is rejected as 4xx, not 500', () => {
    const cases: [string, Record<string, unknown>, number][] = [
      ['negative amount', { amount: '-100.00', currency: 'GBP' }, 400],
      ['zero amount', { amount: '0.00', currency: 'GBP' }, 400],
      ['absurd magnitude', { amount: '9'.repeat(30), currency: 'GBP' }, 400],
      ['unknown currency', { amount: '10.00', currency: 'ZZZ' }, 400],
      ['lowercase currency is normalised', { amount: '10.00', currency: 'gbp' }, 201],
    ];

    it.each(cases)('%s', async (_label, overrides, expected) => {
      await request(app.getHttpServer() as App)
        .post(`/programs/${programRef}/reserve`)
        .set(auth())
        .set('Idempotency-Key', randomUUID())
        .send({
          invoiceRef: `INV-${randomUUID()}`,
          requestedAt: new Date().toISOString(),
          ...overrides,
        })
        .expect(expected);
    });

    it('rejects a future requestedAt', async () => {
      await request(app.getHttpServer() as App)
        .post(`/programs/${programRef}/reserve`)
        .set(auth())
        .set('Idempotency-Key', randomUUID())
        .send({
          invoiceRef: `INV-${randomUUID()}`,
          amount: '10.00',
          currency: 'GBP',
          requestedAt: new Date(Date.now() + 86_400_000).toISOString(),
        })
        .expect(400);
    });

    it('returns 404, not 500, for an unknown programme', async () => {
      await request(app.getHttpServer() as App)
        .get(`/programs/PRG-does-not-exist-${randomUUID()}`)
        .set(auth())
        .expect(404);
    });

    it('rejects a programRef longer than the column allows', async () => {
      await request(app.getHttpServer() as App)
        .get(`/programs/${'x'.repeat(200)}`)
        .set(auth())
        .expect(400);
    });
  });

  it('does not let the client choose the FX rate through requestedAt', async () => {
    // requestedAt is business metadata now; the valuation instant is server
    // time. A caller backdating the request used to select an older, more
    // favourable rate, and that rate was then frozen onto the invoice and
    // replayed at release, making the mispricing permanent.
    const backdated = `INV-${randomUUID()}`;
    const current = `INV-${randomUUID()}`;
    const body = { amount: '500.00', currency: 'GBP' };

    const oldRes = await request(app.getHttpServer() as App)
      .post(`/programs/${programRef}/reserve`)
      .set(auth())
      .set('Idempotency-Key', randomUUID())
      .send({ ...body, invoiceRef: backdated, requestedAt: '2020-01-01T00:00:00.000Z' })
      .expect(201);

    const newRes = await request(app.getHttpServer() as App)
      .post(`/programs/${programRef}/reserve`)
      .set(auth())
      .set('Idempotency-Key', randomUUID())
      .send({ ...body, invoiceRef: current, requestedAt: new Date().toISOString() })
      .expect(201);

    expect((oldRes.body as { reservedAmount: string }).reservedAmount).toBe(
      (newRes.body as { reservedAmount: string }).reservedAmount,
    );
  });
});
