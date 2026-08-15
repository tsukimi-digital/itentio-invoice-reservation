import { Injectable, UnauthorizedException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { JwtService } from '@nestjs/jwt';
import { createHash, randomBytes } from 'node:crypto';
import { PrismaService } from '../prisma/prisma.service';
import { verifyPassword } from './password';
import type { Env } from '../config/env.schema';

export interface JwtClaims {
  sub: string;
  email: string;
  role: string;
  tokenVersion: number;
}

export interface TokenPair {
  accessToken: string;
  refreshToken: string;
}

function hashToken(raw: string): string {
  return createHash('sha256').update(raw).digest('hex');
}

@Injectable()
export class AuthService {
  constructor(
    private readonly prisma: PrismaService,
    private readonly jwt: JwtService,
    private readonly config: ConfigService<Env, true>,
  ) {}

  async login(email: string, password: string): Promise<TokenPair> {
    const user = await this.prisma.user.findUnique({ where: { email } });
    if (!user || !user.isActive || !verifyPassword(password, user.passwordHash)) {
      throw new UnauthorizedException('Invalid credentials');
    }
    return this.issueTokenPair(user);
  }

  /// Rotation-on-use: the presented refresh token is always revoked here,
  /// whether or not the request succeeds past it, so a stolen-and-replayed
  /// token can be used at most once before its owner's next legitimate
  /// refresh fails loudly instead of silently succeeding for an attacker.
  async refresh(rawToken: string): Promise<TokenPair> {
    const tokenHash = hashToken(rawToken);
    const stored = await this.prisma.refreshToken.findUnique({
      where: { tokenHash },
      include: { user: true },
    });
    if (!stored || stored.revokedAt || stored.expiresAt < new Date() || !stored.user.isActive) {
      throw new UnauthorizedException('Invalid refresh token');
    }

    await this.prisma.refreshToken.update({
      where: { id: stored.id },
      data: { revokedAt: new Date() },
    });

    return this.issueTokenPair(stored.user);
  }

  private async issueTokenPair(user: {
    id: string;
    email: string;
    role: string;
    tokenVersion: number;
  }): Promise<TokenPair> {
    const claims: JwtClaims = {
      sub: user.id,
      email: user.email,
      role: user.role,
      tokenVersion: user.tokenVersion,
    };
    const accessToken = await this.jwt.signAsync(claims);

    const rawRefreshToken = randomBytes(32).toString('base64url');
    const ttlDays = this.config.get('REFRESH_TOKEN_TTL_DAYS', { infer: true });
    await this.prisma.refreshToken.create({
      data: {
        userId: user.id,
        tokenHash: hashToken(rawRefreshToken),
        expiresAt: new Date(Date.now() + ttlDays * 24 * 60 * 60 * 1000),
      },
    });

    return { accessToken, refreshToken: rawRefreshToken };
  }
}
