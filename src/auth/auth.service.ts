import { Injectable, Logger, UnauthorizedException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { JwtService } from '@nestjs/jwt';
import { Prisma } from '@prisma/client';
import { createHash, randomBytes } from 'node:crypto';
import { PrismaService } from '../prisma/prisma.service';
import { hashPassword, verifyPassword } from './password';
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

/// A real scrypt hash of a value nobody knows, computed once per process.
///
/// `login` verifies against this when the e-mail does not exist, so the miss
/// path pays the same ~80ms KDF cost as a hit. Without it the short-circuit
/// `!user || ... || verifyPassword(...)` answered an unknown address in about
/// a millisecond and a known one two orders of magnitude slower — a reliable
/// account-enumeration oracle, identical status code notwithstanding.
const DUMMY_PASSWORD_HASH = hashPassword(randomBytes(32).toString('hex'));

@Injectable()
export class AuthService {
  private readonly logger = new Logger(AuthService.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly jwt: JwtService,
    private readonly config: ConfigService<Env, true>,
  ) {}

  async login(email: string, password: string): Promise<TokenPair> {
    const user = await this.prisma.user.findUnique({ where: { email } });

    // Always run the KDF, even for an unknown address — see DUMMY_PASSWORD_HASH.
    const passwordMatches = verifyPassword(password, user?.passwordHash ?? DUMMY_PASSWORD_HASH);

    if (!user || !user.isActive || !passwordMatches) {
      throw new UnauthorizedException('Invalid credentials');
    }

    await this.prisma.user.update({
      where: { id: user.id },
      data: { lastLoginAt: new Date() },
    });

    return this.issueTokenPair(user);
  }

  /// Rotation-on-use with reuse detection.
  ///
  /// Two things were wrong before. The revocation was a read followed by a
  /// separate unconditional write, so two concurrent refreshes with the same
  /// token both observed `revokedAt = null` and both walked away with a valid,
  /// independent token chain — the "usable at most once" guarantee in ADR-018
  /// simply did not hold under concurrency. And replaying an already-revoked
  /// token merely returned 401: the attacker who rotated first kept a live
  /// session that renewed itself indefinitely, while the legitimate client's
  /// failure was the only signal and nothing acted on it.
  ///
  /// Now the revoke is a conditional UPDATE (exactly one caller can win), and
  /// presenting a revoked token revokes the user's whole token family. See
  /// docs/DECISIONS.md ADR-031.
  async refresh(rawToken: string): Promise<TokenPair> {
    const tokenHash = hashToken(rawToken);
    const stored = await this.prisma.refreshToken.findUnique({
      where: { tokenHash },
      include: { user: true },
    });
    if (!stored) {
      throw new UnauthorizedException('Invalid refresh token');
    }

    if (stored.revokedAt) {
      // A token is only revoked by a successful rotation or by this branch.
      // Seeing one again means the value leaked: assume compromise and end
      // every session for that user.
      const killed = await this.revokeAllForUser(stored.userId);
      this.logger.warn(
        `Refresh token reuse detected for user ${stored.userId}; revoked ${killed} active token(s)`,
      );
      throw new UnauthorizedException('Invalid refresh token');
    }

    if (stored.expiresAt < new Date() || !stored.user.isActive) {
      throw new UnauthorizedException('Invalid refresh token');
    }

    return this.prisma.$transaction(async (tx) => {
      const claimed = await tx.refreshToken.updateMany({
        where: { id: stored.id, revokedAt: null },
        data: { revokedAt: new Date() },
      });
      // Zero rows means a concurrent request already rotated this token.
      if (claimed.count !== 1) {
        throw new UnauthorizedException('Invalid refresh token');
      }
      return this.issueTokenPair(stored.user, tx);
    });
  }

  private revokeAllForUser(userId: string): Promise<number> {
    return this.prisma.refreshToken
      .updateMany({
        where: { userId, revokedAt: null },
        data: { revokedAt: new Date() },
      })
      .then((result) => result.count);
  }

  private async issueTokenPair(
    user: {
      id: string;
      email: string;
      role: string;
      tokenVersion: number;
    },
    client: Prisma.TransactionClient = this.prisma,
  ): Promise<TokenPair> {
    const claims: JwtClaims = {
      sub: user.id,
      email: user.email,
      role: user.role,
      tokenVersion: user.tokenVersion,
    };
    const accessToken = await this.jwt.signAsync(claims);

    const rawRefreshToken = randomBytes(32).toString('base64url');
    const ttlDays = this.config.get('REFRESH_TOKEN_TTL_DAYS', { infer: true });
    // Created on the same client that revoked the predecessor, so a failure
    // between the two cannot leave a session revoked with no replacement.
    await client.refreshToken.create({
      data: {
        userId: user.id,
        tokenHash: hashToken(rawRefreshToken),
        expiresAt: new Date(Date.now() + ttlDays * 24 * 60 * 60 * 1000),
      },
    });

    return { accessToken, refreshToken: rawRefreshToken };
  }
}
