import { INestApplication } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import request from 'supertest';
import type { App } from 'supertest/types';
import { AppModule } from '../src/app.module';
import { configureApp } from '../src/bootstrap';

describe('Refresh token (e2e)', () => {
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

  async function login(): Promise<{ accessToken: string; refreshToken: string }> {
    const res = await request(app.getHttpServer() as App)
      .post('/auth/login')
      .send({ email: 'admin@itentio.dev', password: 'dev-only-password-change-me' })
      .expect(200);
    return res.body as { accessToken: string; refreshToken: string };
  }

  it('returns a refresh token alongside the access token on login', async () => {
    const { accessToken, refreshToken } = await login();
    expect(accessToken).toEqual(expect.any(String));
    expect(refreshToken).toEqual(expect.any(String));
  });

  it('exchanges a valid refresh token for a new access token + refresh token', async () => {
    const { refreshToken } = await login();

    const res = await request(app.getHttpServer() as App)
      .post('/auth/refresh')
      .send({ refreshToken })
      .expect(200);

    const body = res.body as { accessToken: string; refreshToken: string };
    expect(body.accessToken).toEqual(expect.any(String));
    expect(body.refreshToken).toEqual(expect.any(String));
    expect(body.refreshToken).not.toBe(refreshToken);
  });

  it('rejects reuse of an already-rotated refresh token', async () => {
    const { refreshToken } = await login();

    await request(app.getHttpServer() as App)
      .post('/auth/refresh')
      .send({ refreshToken })
      .expect(200);

    return request(app.getHttpServer() as App)
      .post('/auth/refresh')
      .send({ refreshToken })
      .expect(401);
  });

  it('rejects an unknown refresh token', () => {
    return request(app.getHttpServer() as App)
      .post('/auth/refresh')
      .send({ refreshToken: 'not-a-real-token' })
      .expect(401);
  });

  it('the new access token from a refresh works against a protected route', async () => {
    const { refreshToken } = await login();
    const res = await request(app.getHttpServer() as App)
      .post('/auth/refresh')
      .send({ refreshToken })
      .expect(200);
    const { accessToken } = res.body as { accessToken: string };

    // A nonexistent program correctly 404s rather than 401ing — proving the
    // token passed JwtAuthGuard, not merely that the route is reachable.
    return request(app.getHttpServer() as App)
      .get('/programs/PRG-DOES-NOT-EXIST')
      .set('Authorization', `Bearer ${accessToken}`)
      .expect(404);
  });
});
