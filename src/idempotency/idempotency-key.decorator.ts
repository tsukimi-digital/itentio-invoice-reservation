import { BadRequestException, createParamDecorator, ExecutionContext } from '@nestjs/common';
import { createHash, randomUUID } from 'node:crypto';
import type { IdempotencyContext } from './idempotency';
import type { JwtClaims } from '../auth/auth.service';

interface RequestWithIdempotencyInputs {
  headers: Record<string, string | undefined>;
  body: unknown;
  method: string;
  route: { path: string };
  user?: JwtClaims;
}

/// Extracts the Idempotency-Key header into a ready-to-claim context. The
/// request body is canonicalised (stable key order) before hashing so
/// semantically-identical JSON with different key order still matches.
export const IdempotencyCtx = createParamDecorator(
  (_: unknown, ctx: ExecutionContext): IdempotencyContext => {
    const req = ctx.switchToHttp().getRequest<RequestWithIdempotencyInputs>();
    const key = req.headers['idempotency-key'];
    if (!key) {
      throw new BadRequestException('Idempotency-Key header is required');
    }
    const clientId = req.user?.sub ?? 'anonymous';
    const body = (req.body ?? {}) as Record<string, unknown>;
    const canonical = JSON.stringify(body, Object.keys(body).sort());
    const requestHash = createHash('sha256').update(canonical).digest('hex');
    return {
      rowId: randomUUID(),
      clientId,
      key,
      method: req.method,
      path: req.route.path,
      requestHash,
    };
  },
);
