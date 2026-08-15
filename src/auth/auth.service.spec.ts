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
  const refreshTokenCreate = jest.fn().mockResolvedValue({});
  const refreshTokenFindUnique = jest.fn();
  const refreshTokenUpdate = jest.fn().mockResolvedValue({});
  const prismaMock = {
    user: { findUnique },
    refreshToken: {
      create: refreshTokenCreate,
      findUnique: refreshTokenFindUnique,
      update: refreshTokenUpdate,
    },
  };
  const configMock = { get: jest.fn().mockReturnValue(30) };

  let service: AuthService;

  beforeEach(async () => {
    jest.clearAllMocks();
    findUnique.mockResolvedValue(user);
    refreshTokenCreate.mockResolvedValue({});
    refreshTokenUpdate.mockResolvedValue({});
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

  it('rotates a valid refresh token: revokes the old row, issues a new pair', async () => {
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
    expect(refreshTokenUpdate).toHaveBeenCalledWith({
      where: { id: 'rt1' },
      data: { revokedAt: expect.any(Date) as Date },
    });
    expect(refreshTokenCreate).toHaveBeenCalledTimes(1);
  });

  it('rejects an unknown refresh token', async () => {
    refreshTokenFindUnique.mockResolvedValue(null);
    await expect(service.refresh('unknown-token')).rejects.toThrow('Invalid refresh token');
  });

  it('rejects a revoked refresh token', async () => {
    refreshTokenFindUnique.mockResolvedValue({
      id: 'rt1',
      userId: 'u1',
      expiresAt: new Date(Date.now() + 1000 * 60 * 60),
      revokedAt: new Date(),
      user,
    });
    await expect(service.refresh('revoked-token')).rejects.toThrow('Invalid refresh token');
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
