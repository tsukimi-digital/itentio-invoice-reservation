import { INestApplication } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import request from 'supertest';
import type { App } from 'supertest/types';
import { randomUUID } from 'node:crypto';
import { AppModule } from '../src/app.module';
import { PrismaService } from '../src/prisma/prisma.service';
import { hashPassword } from '../src/auth/password';

describe('RBAC (e2e)', () => {
  let app: INestApplication;
  let prisma: PrismaService;
  let readerToken: string;
  let operatorToken: string;
  let adminToken: string;
  // Unique per test run — fixed refs would collide with idempotency-key rows
  // and invoices left behind by a previous run of this same spec.
  const programRef = `PRG-RBAC-${randomUUID()}`;

  async function loginAs(email: string): Promise<string> {
    const res = await request(app.getHttpServer() as App)
      .post('/auth/login')
      .send({ email, password: 'rbac-test-password' });
    return (res.body as { accessToken: string }).accessToken;
  }

  beforeAll(async () => {
    const moduleFixture = await Test.createTestingModule({ imports: [AppModule] }).compile();
    app = moduleFixture.createNestApplication();
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
    await prisma.user.upsert({
      where: { email: 'reader@itentio.dev' },
      update: {},
      create: { email: 'reader@itentio.dev', passwordHash, displayName: 'Reader', role: 'READER' },
    });
    await prisma.user.upsert({
      where: { email: 'operator@itentio.dev' },
      update: {},
      create: {
        email: 'operator@itentio.dev',
        passwordHash,
        displayName: 'Operator',
        role: 'OPERATOR',
      },
    });
    await prisma.user.upsert({
      where: { email: 'rbac-admin@itentio.dev' },
      update: {},
      create: {
        email: 'rbac-admin@itentio.dev',
        passwordHash,
        displayName: 'Admin',
        role: 'ADMIN',
      },
    });

    readerToken = await loginAs('reader@itentio.dev');
    operatorToken = await loginAs('operator@itentio.dev');
    adminToken = await loginAs('rbac-admin@itentio.dev');
  });

  afterAll(async () => {
    await app.close();
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
