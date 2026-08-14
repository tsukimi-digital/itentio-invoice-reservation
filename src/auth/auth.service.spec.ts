import { Test } from '@nestjs/testing';
import { JwtModule } from '@nestjs/jwt';
import { AuthService } from './auth.service';
import { PrismaService } from '../prisma/prisma.service';
import { hashPassword } from './password';

describe('AuthService', () => {
  const passwordHash = hashPassword('correct-horse');
  const findUnique = jest.fn().mockResolvedValue({
    id: 'u1',
    email: 'a@b.com',
    passwordHash,
    isActive: true,
    role: 'ADMIN',
    tokenVersion: 0,
  });
  const prismaMock = { user: { findUnique } };

  let service: AuthService;

  beforeEach(async () => {
    const module = await Test.createTestingModule({
      imports: [
        JwtModule.register({
          secret: 'test-secret-at-least-16-chars',
          signOptions: { expiresIn: '1h' },
        }),
      ],
      providers: [AuthService, { provide: PrismaService, useValue: prismaMock }],
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
});
