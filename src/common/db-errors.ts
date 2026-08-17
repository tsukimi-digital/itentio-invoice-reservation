import { Prisma } from '@prisma/client';

/// Prisma error codes that mean "the database or the connection to it is
/// temporarily unhappy", as opposed to "this request will never succeed".
const TRANSIENT_PRISMA_CODES = new Set([
  'P1001', // can't reach database server
  'P1002', // database server timed out
  'P1008', // operation timed out
  'P1017', // server closed the connection
  'P2024', // connection pool timeout
  'P2028', // transaction API error
  'P2034', // write conflict / deadlock
]);

/// Postgres SQLSTATEs, surfaced by Prisma inside `meta.code` on raw queries.
const TRANSIENT_SQLSTATES = new Set([
  '40001', // serialization failure
  '40P01', // deadlock detected
  '53300', // too many connections
  '55P03', // lock not available (our SET LOCAL lock_timeout tripping)
  '57P01', // admin shutdown
  '08000', // connection exception
  '08003', // connection does not exist
  '08006', // connection failure
]);

/// Classifies a failure as retryable.
///
/// Most errors the Kafka consumer sees are deterministic — a CHECK violation,
/// an unknown programme, a malformed numeric — and fail identically on every
/// redelivery. Rethrowing them all so kafkajs retries stops the partition
/// advancing at all, so only the codes above are treated as retryable.
///
/// The default is deliberately `false` (permanent). An unknown deterministic
/// error retried forever wedges the whole partition; the same error parked in
/// the dead-letter table is visible, inspectable and costs one message. See
/// docs/DECISIONS.md ADR-10.
export function isTransientDbError(err: unknown): boolean {
  if (err instanceof Prisma.PrismaClientInitializationError) return true;
  if (err instanceof Prisma.PrismaClientRustPanicError) return true;

  if (err instanceof Prisma.PrismaClientKnownRequestError) {
    if (TRANSIENT_PRISMA_CODES.has(err.code)) return true;
    const sqlState = (err.meta as { code?: string } | undefined)?.code;
    return sqlState !== undefined && TRANSIENT_SQLSTATES.has(sqlState);
  }

  return false;
}

/// Raised by handlers for input that is well-formed enough to parse but can
/// never be applied — a treasury delta whose currency contradicts the
/// programme, for instance. Always dead-lettered, never retried.
export class PermanentEventError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'PermanentEventError';
  }
}
