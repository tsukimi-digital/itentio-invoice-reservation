import { BadRequestException, createParamDecorator, ExecutionContext } from '@nestjs/common';
import { createHash, randomUUID } from 'node:crypto';
import type { IdempotencyContext } from './idempotency';
import type { JwtClaims } from '../auth/auth.service';

interface RequestWithIdempotencyInputs {
  headers: Record<string, string | undefined>;
  body: unknown;
  method: string;
  originalUrl: string;
  route: { path: string };
  user?: JwtClaims;
}

const MAX_KEY_LENGTH = 255;

/// Nesting depth accepted in a request body. The DTOs on these endpoints are
/// flat, so this is far above anything legitimate; it exists because
/// `canonicalize` recurses and the fingerprint is computed during parameter
/// resolution, i.e. BEFORE ValidationPipe gets to reject the unknown fields. An
/// unbounded walk therefore turns a few kilobytes of nested JSON into a stack
/// overflow, which surfaces as a 500 on a request that should be a 400.
const MAX_BODY_DEPTH = 32;

/// Stable JSON with keys sorted at EVERY depth.
///
/// The replacer-*array* form of `JSON.stringify` is deliberately not used: it
/// is a key allow-list applied at every nesting level rather than a sort, so it
/// would drop nested fields from the hash and let two materially different
/// requests fingerprint identically.
function canonicalize(value: unknown, depth = 0): unknown {
  if (depth > MAX_BODY_DEPTH) {
    throw new BadRequestException(`Request body nests deeper than ${MAX_BODY_DEPTH} levels`);
  }
  if (Array.isArray(value)) return value.map((item) => canonicalize(item, depth + 1));
  if (value !== null && typeof value === 'object') {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>)
        .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
        .map(([key, nested]) => [key, canonicalize(nested, depth + 1)]),
    );
  }
  return value;
}

/// Extracts the Idempotency-Key header into a ready-to-claim context.
///
/// The fingerprint covers the METHOD and the concrete URL as well as the body.
/// Hashing the body alone scopes the key too widely and silently skips work:
/// `POST /programs/PRG-A/reserve` and `POST /programs/PRG-B/reserve` carry
/// byte-identical bodies (programRef is a path parameter), so the second
/// request would replay the first programme's response — 201 with the wrong
/// invoiceId — and PRG-B would never be debited. `req.route.path` cannot
/// substitute for this: it is the route *template*
/// (`/programs/:programRef/reserve`), identical for every programme. See
/// docs/DECISIONS.md ADR-08.
export const IdempotencyCtx = createParamDecorator(
  (_: unknown, ctx: ExecutionContext): IdempotencyContext => {
    const req = ctx.switchToHttp().getRequest<RequestWithIdempotencyInputs>();
    const key = req.headers['idempotency-key'];
    if (!key) {
      throw new BadRequestException('Idempotency-Key header is required');
    }
    if (key.length > MAX_KEY_LENGTH) {
      throw new BadRequestException(`Idempotency-Key must be at most ${MAX_KEY_LENGTH} characters`);
    }

    // `user` is always populated: every route carrying this decorator sits
    // behind the global JwtAuthGuard. The fallback exists only so an
    // unauthenticated route added later cannot quietly put every caller into
    // one shared idempotency namespace.
    const clientId = req.user?.sub;
    if (!clientId) {
      throw new BadRequestException('Idempotency-Key requires an authenticated caller');
    }

    // Query string stripped: it carries no routing meaning for these endpoints
    // and would otherwise let `?x=1` masquerade as a different request.
    const path = req.originalUrl.split('?')[0];
    const canonical = JSON.stringify({
      method: req.method,
      path,
      body: canonicalize(req.body ?? {}),
    });

    return {
      rowId: randomUUID(),
      clientId,
      key,
      method: req.method,
      path,
      requestHash: createHash('sha256').update(canonical).digest('hex'),
    };
  },
);
