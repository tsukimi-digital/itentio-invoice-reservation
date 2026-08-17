import { Test } from '@nestjs/testing';
import { ConfigService } from '@nestjs/config';
import { JwtModule } from '@nestjs/jwt';
import { AuthService } from './auth.service';
import { PrismaService } from '../prisma/prisma.service';
import { hashPassword } from './password';

describe('AuthService', () => {
  const passwordHash = hashPassword('correct-horse');
  const user = {
    id: 'u1',
    email: 'a@b.com',
    passwordHash,
    isActive: true,
    role: 'ADMIN',
    tokenVersion: 0,
  };
  const findUnique = jest.fn().mockResolvedValue(user);
  const userUpdate = jest.fn().mockResolvedValue(user);
  const refreshTokenCreate = jest.fn().mockResolvedValue({});
  const refreshTokenFindUnique = jest.fn();
  const refreshTokenUpdateMany = jest.fn().mockResolvedValue({ count: 1 });
  const prismaMock = {
    user: { findUnique, update: userUpdate },
    refreshToken: {
      create: refreshTokenCreate,
      findUnique: refreshTokenFindUnique,
      updateMany: refreshTokenUpdateMany,
    },
    // Interactive transaction, wired in beforeEach to run inline against this
    // same mock (declared empty here to avoid a self-referential initialiser).
    $transaction: jest.fn(),
  };
  const configMock = { get: jest.fn().mockReturnValue(30) };

  let service: AuthService;

  beforeEach(async () => {
    jest.clearAllMocks();
    findUnique.mockResolvedValue(user);
    userUpdate.mockResolvedValue(user);
    refreshTokenCreate.mockResolvedValue({});
    refreshTokenUpdateMany.mockResolvedValue({ count: 1 });
    prismaMock.$transaction.mockImplementation((fn: (tx: unknown) => unknown) => fn(prismaMock));
    configMock.get.mockReturnValue(30);

    const module = await Test.createTestingModule({
      imports: [
        JwtModule.register({
          secret: 'test-secret-at-least-16-chars',
          signOptions: { expiresIn: '1h' },
        }),
      ],
      providers: [
        AuthService,
        { provide: PrismaService, useValue: prismaMock },
        { provide: ConfigService, useValue: configMock },
      ],
    }).compile();
    service = module.get(AuthService);
  });

  it('issues a JWT for correct credentials', async () => {
    const result = await service.login('a@b.com', 'correct-horse');
    expect(result.accessToken).toEqual(expect.any(String));
  });

  it('rejects an incorrect password', async () => {
    await expect(service.login('a@b.com', 'wrong')).rejects.toThrow('Invalid credentials');
  });

  it('issues an opaque refresh token alongside the access token on login', async () => {
    const result = await service.login('a@b.com', 'correct-horse');
    expect(result.refreshToken).toEqual(expect.any(String));
    expect(refreshTokenCreate).toHaveBeenCalledTimes(1);
  });

  it('records the login timestamp', async () => {
    await service.login('a@b.com', 'correct-horse');
    expect(userUpdate).toHaveBeenCalledWith({
      where: { id: 'u1' },
      data: { lastLoginAt: expect.any(Date) as Date },
    });
  });

  // The enumeration oracle here is latency, not the status code: short-circuiting
  // on an unknown address skips the KDF and answers roughly 80x faster than a
  // known one. Asserting on wall-clock time would be flaky, so this pins the
  // observable cause instead — no user row is found, yet the rejection still
  // pays for the same key derivation.
  it('rejects an unknown e-mail without revealing it through a fast path', async () => {
    findUnique.mockResolvedValue(null);

    const started = process.hrtime.bigint();
    await expect(service.login('nobody@b.com', 'whatever')).rejects.toThrow('Invalid credentials');
    const unknownNs = process.hrtime.bigint() - started;

    findUnique.mockResolvedValue(user);
    const started2 = process.hrtime.bigint();
    await expect(service.login('a@b.com', 'wrong')).rejects.toThrow('Invalid credentials');
    const knownNs = process.hrtime.bigint() - started2;

    // Both paths run a full scrypt derivation, so neither is an order of
    // magnitude faster than the other. A 10x band is loose enough to survive
    // a noisy CI runner and tight enough to catch the short-circuit, which
    // was ~80x.
    const ratio = Number(knownNs) / Number(unknownNs);
    expect(ratio).toBeLessThan(10);
    expect(ratio).toBeGreaterThan(0.1);
  });

  it('rotates a valid refresh token: revokes the old row conditionally, issues a new pair', async () => {
    refreshTokenFindUnique.mockResolvedValue({
      id: 'rt1',
      userId: 'u1',
      expiresAt: new Date(Date.now() + 1000 * 60 * 60),
      revokedAt: null,
      user,
    });

    const result = await service.refresh('some-raw-refresh-token');
    expect(result.accessToken).toEqual(expect.any(String));
    expect(result.refreshToken).toEqual(expect.any(String));
    // Conditional on revokedAt: null — this is what makes two concurrent
    // refreshes with the same token produce exactly one winner.
    expect(refreshTokenUpdateMany).toHaveBeenCalledWith({
      where: { id: 'rt1', revokedAt: null },
      data: { revokedAt: expect.any(Date) as Date },
    });
    expect(refreshTokenCreate).toHaveBeenCalledTimes(1);
  });

  it('loses the race gracefully when a concurrent refresh already rotated the token', async () => {
    refreshTokenFindUnique.mockResolvedValue({
      id: 'rt1',
      userId: 'u1',
      expiresAt: new Date(Date.now() + 1000 * 60 * 60),
      revokedAt: null,
      user,
    });
    refreshTokenUpdateMany.mockResolvedValue({ count: 0 });

    await expect(service.refresh('some-raw-refresh-token')).rejects.toThrow(
      'Invalid refresh token',
    );
    expect(refreshTokenCreate).not.toHaveBeenCalled();
  });

  it('rejects an unknown refresh token', async () => {
    refreshTokenFindUnique.mockResolvedValue(null);
    await expect(service.refresh('unknown-token')).rejects.toThrow('Invalid refresh token');
  });

  it('treats a revoked refresh token as a leak and revokes the whole family', async () => {
    refreshTokenFindUnique.mockResolvedValue({
      id: 'rt1',
      userId: 'u1',
      expiresAt: new Date(Date.now() + 1000 * 60 * 60),
      revokedAt: new Date(),
      user,
    });
    refreshTokenUpdateMany.mockResolvedValue({ count: 3 });

    await expect(service.refresh('revoked-token')).rejects.toThrow('Invalid refresh token');
    expect(refreshTokenUpdateMany).toHaveBeenCalledWith({
      where: { userId: 'u1', revokedAt: null },
      data: { revokedAt: expect.any(Date) as Date },
    });
    expect(refreshTokenCreate).not.toHaveBeenCalled();
  });

  it('rejects an expired refresh token', async () => {
    refreshTokenFindUnique.mockResolvedValue({
      id: 'rt1',
      userId: 'u1',
      expiresAt: new Date(Date.now() - 1000),
      revokedAt: null,
      user,
    });
    await expect(service.refresh('expired-token')).rejects.toThrow('Invalid refresh token');
  });
});
