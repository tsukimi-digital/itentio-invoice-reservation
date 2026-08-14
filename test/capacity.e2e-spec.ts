import { INestApplication } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import request from 'supertest';
import type { App } from 'supertest/types';
import { AppModule } from '../src/app.module';
import { PrismaService } from '../src/prisma/prisma.service';

describe('Capacity (e2e)', () => {
  let app: INestApplication;
  let prisma: PrismaService;
  let token: string;

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
    await prisma.program.upsert({
      where: { externalRef: 'PRG-E2E' },
      update: {},
      create: { externalRef: 'PRG-E2E', name: 'E2E', currencyCode: 'GBP', totalLimit: '1000.00' },
    });

    const login = await request(app.getHttpServer() as App)
      .post('/auth/login')
      .send({ email: 'admin@itentio.dev', password: 'dev-only-password-change-me' });
    token = (login.body as { accessToken: string }).accessToken;
  });

  afterAll(async () => {
    await app.close();
  });

  it('rejects an unauthenticated availability request', () => {
    return request(app.getHttpServer() as App)
      .get('/programs/PRG-E2E')
      .expect(401);
  });

  it('returns current availability for an authenticated request', async () => {
    const res = await request(app.getHttpServer() as App)
      .get('/programs/PRG-E2E')
      .set('Authorization', `Bearer ${token}`)
      .expect(200);
    expect(res.body).toEqual({
      programRef: 'PRG-E2E',
      currency: 'GBP',
      totalLimit: '1000.00',
      reservedAmount: '0.00',
      available: '1000.00',
    });
  });
});
