import { INestApplication } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import request from 'supertest';
import type { App } from 'supertest/types';
import { AppModule } from '../src/app.module';
import { configureApp } from '../src/bootstrap';

describe('Rate limiting (e2e)', () => {
  let app: INestApplication;

  beforeAll(async () => {
    const moduleFixture = await Test.createTestingModule({ imports: [AppModule] }).compile();
    app = moduleFixture.createNestApplication();
    // Same global pipes, filters and interceptors the real process installs.
    configureApp(app);
    await app.init();
  });

  afterAll(async () => {
    await app.close();
  });

  it('throttles /auth/login after 5 requests within a minute', async () => {
    const attempt = () =>
      request(app.getHttpServer() as App)
        .post('/auth/login')
        .send({ email: 'admin@itentio.dev', password: 'wrong-password' });

    const statuses: number[] = [];
    for (let i = 0; i < 6; i++) {
      const res = await attempt();
      statuses.push(res.status);
    }

    // First 5 requests are rejected on credentials (401), not throttled.
    expect(statuses.slice(0, 5)).toEqual([401, 401, 401, 401, 401]);
    // The 6th request within the same window is throttled, not credential-checked.
    expect(statuses[5]).toBe(429);
  });
});
