import { INestApplication } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import request from 'supertest';
import type { App } from 'supertest/types';
import { randomUUID } from 'node:crypto';
import { AppModule } from '../src/app.module';
import { configureApp } from '../src/bootstrap';
import { PrismaService } from '../src/prisma/prisma.service';
import { hashPassword } from '../src/auth/password';

describe('RBAC (e2e)', () => {
  let app: INestApplication;
  let prisma: PrismaService;
  let readerToken: string;
  let operatorToken: string;
  let adminToken: string;
  // Unique per test run. Fixed identifiers collide with rows left behind by a
  // previous run against the same database: not only idempotency keys and
  // invoices, but the fixture users too — `upsert` with an empty `update`
  // leaves an existing row's password hash in place, so a generic address like
  // `reader@itentio.dev` created with any other password makes every login
  // here return 401 and the whole spec fail for reasons unrelated to RBAC.
  const runId = randomUUID();
  const programRef = `PRG-RBAC-${runId}`;
  const readerEmail = `rbac-reader-${runId}@itentio.dev`;
  const operatorEmail = `rbac-operator-${runId}@itentio.dev`;
  const adminEmail = `rbac-admin-${runId}@itentio.dev`;

  async function loginAs(email: string): Promise<string> {
    const res = await request(app.getHttpServer() as App)
      .post('/auth/login')
      .send({ email, password: 'rbac-test-password' })
      // Asserted, so a failed login fails here with its real status instead of
      // handing every later request `Bearer undefined` and a confusing 401.
      .expect(200);
    return (res.body as { accessToken: string }).accessToken;
  }

  beforeAll(async () => {
    const moduleFixture = await Test.createTestingModule({ imports: [AppModule] }).compile();
    app = moduleFixture.createNestApplication();
    // Same global pipes, filters and interceptors the real process installs.
    configureApp(app);
    await app.init();
    prisma = app.get(PrismaService);

    await prisma.currency.upsert({
      where: { code: 'GBP' },
      update: {},
      create: { code: 'GBP', minorUnits: 2, name: 'GBP' },
    });
    await prisma.program.create({
      data: { externalRef: programRef, name: 'RBAC', currencyCode: 'GBP', totalLimit: '1000.00' },
    });

    const passwordHash = hashPassword('rbac-test-password');
    await prisma.user.createMany({
      data: [
        { email: readerEmail, passwordHash, displayName: 'Reader', role: 'READER' },
        { email: operatorEmail, passwordHash, displayName: 'Operator', role: 'OPERATOR' },
        { email: adminEmail, passwordHash, displayName: 'Admin', role: 'ADMIN' },
      ],
    });

    readerToken = await loginAs(readerEmail);
    operatorToken = await loginAs(operatorEmail);
    adminToken = await loginAs(adminEmail);
  });

  afterAll(async () => {
    // Fixtures are unique per run, so they would otherwise accumulate in a
    // persistent local database. Logging in issued refresh tokens that
    // reference these users, so those go first. `app.close()` runs in `finally`
    // — a failed cleanup must not leave the Kafka consumer connected, which
    // stops Jest from exiting at all.
    try {
      const ids = (
        await prisma.user.findMany({
          where: { email: { in: [readerEmail, operatorEmail, adminEmail] } },
          select: { id: true },
        })
      ).map((user) => user.id);
      await prisma.refreshToken.deleteMany({ where: { userId: { in: ids } } });
      await prisma.user.deleteMany({ where: { id: { in: ids } } });
    } finally {
      await app.close();
    }
  });

  it('lets a READER query availability', () => {
    return request(app.getHttpServer() as App)
      .get(`/programs/${programRef}`)
      .set('Authorization', `Bearer ${readerToken}`)
      .expect(200);
  });

  it('blocks a READER from reserving capacity', () => {
    return request(app.getHttpServer() as App)
      .post(`/programs/${programRef}/reserve`)
      .set('Authorization', `Bearer ${readerToken}`)
      .set('Idempotency-Key', randomUUID())
      .send({
        invoiceRef: `INV-${randomUUID()}`,
        amount: '10.00',
        currency: 'GBP',
        requestedAt: new Date().toISOString(),
      })
      .expect(403);
  });

  it('lets an OPERATOR reserve capacity', () => {
    return request(app.getHttpServer() as App)
      .post(`/programs/${programRef}/reserve`)
      .set('Authorization', `Bearer ${operatorToken}`)
      .set('Idempotency-Key', randomUUID())
      .send({
        invoiceRef: `INV-${randomUUID()}`,
        amount: '10.00',
        currency: 'GBP',
        requestedAt: new Date().toISOString(),
      })
      .expect(201);
  });

  it('lets an ADMIN release capacity', async () => {
    const invoiceRef = `INV-${randomUUID()}`;
    await request(app.getHttpServer() as App)
      .post(`/programs/${programRef}/reserve`)
      .set('Authorization', `Bearer ${adminToken}`)
      .set('Idempotency-Key', randomUUID())
      .send({ invoiceRef, amount: '10.00', currency: 'GBP', requestedAt: new Date().toISOString() })
      .expect(201);

    return request(app.getHttpServer() as App)
      .post(`/programs/${programRef}/release`)
      .set('Authorization', `Bearer ${adminToken}`)
      .set('Idempotency-Key', randomUUID())
      .send({ invoiceRef })
      .expect(201);
  });

  it('blocks a READER from releasing capacity', async () => {
    const invoiceRef = `INV-${randomUUID()}`;
    await request(app.getHttpServer() as App)
      .post(`/programs/${programRef}/reserve`)
      .set('Authorization', `Bearer ${operatorToken}`)
      .set('Idempotency-Key', randomUUID())
      .send({ invoiceRef, amount: '10.00', currency: 'GBP', requestedAt: new Date().toISOString() })
      .expect(201);

    return request(app.getHttpServer() as App)
      .post(`/programs/${programRef}/release`)
      .set('Authorization', `Bearer ${readerToken}`)
      .set('Idempotency-Key', randomUUID())
      .send({ invoiceRef })
      .expect(403);
  });
});
