import { IdempotencyStatus, type Prisma, type PrismaClient } from '@prisma/client';

/// Recorded on the key row but never read back — the replay's HTTP status comes
/// from Nest, not from here. See docs/DECISIONS.md ADR-13.
const RECORDED_REPLAY_STATUS = 200;

/// How long a key row advertises itself as replayable. ADR-13 records that the
/// column is written but not yet enforced by a sweeper.
const KEY_TTL_HOURS = 24;

export type Tx = Omit<
  PrismaClient,
  '$connect' | '$disconnect' | '$on' | '$transaction' | '$use' | '$extends'
>;

export interface IdempotencyContext {
  rowId: string;
  clientId: string;
  key: string;
  method: string;
  path: string;
  requestHash: string;
}

export type Claim =
  | { kind: 'owned' }
  | { kind: 'replay'; storedResponse: unknown }
  | { kind: 'conflict' }
  | { kind: 'in_flight' };

/// The idempotency-key claim is always the FIRST lock taken in any write
/// transaction (see docs/DECISIONS.md — global lock order idempotency_key ->
/// program -> invoice), so two identical concurrent requests serialize on the
/// key itself rather than racing further downstream.
export async function claimIdempotencyKey(tx: Tx, ctx: IdempotencyContext): Promise<Claim> {
  const inserted: number = await tx.$executeRaw`
    INSERT INTO idempotency_key
      (id, client_id, key, method, path, request_hash, status, expires_at, created_at)
    VALUES (${ctx.rowId}::uuid, ${ctx.clientId}, ${ctx.key}, ${ctx.method},
            ${ctx.path}, ${ctx.requestHash},
            ${IdempotencyStatus.IN_FLIGHT}::"IdempotencyStatus",
            now() + make_interval(hours => ${KEY_TTL_HOURS}::int), now())
    ON CONFLICT (client_id, key) DO NOTHING
  `;
  if (inserted === 1) return { kind: 'owned' };

  const existing = await tx.idempotencyKey.findUniqueOrThrow({
    where: { clientId_key: { clientId: ctx.clientId, key: ctx.key } },
  });
  if (existing.requestHash !== ctx.requestHash) return { kind: 'conflict' };
  if (existing.status === IdempotencyStatus.COMPLETED)
    return { kind: 'replay', storedResponse: existing.responseBody };
  return { kind: 'in_flight' };
}

export async function finishIdempotent<T>(
  tx: Tx,
  ctx: IdempotencyContext,
  response: T,
): Promise<T> {
  await tx.idempotencyKey.update({
    where: { clientId_key: { clientId: ctx.clientId, key: ctx.key } },
    data: {
      status: IdempotencyStatus.COMPLETED,
      responseStatus: RECORDED_REPLAY_STATUS,
      responseBody: response as Prisma.InputJsonValue,
      completedAt: new Date(),
    },
  });
  return response;
}
