import { z } from 'zod';

// Multi-schema topic (deltas + snapshots) via a oneOf-style discriminated
// union, so an unknown event_type fails closed rather than being coerced.

/// The wire contract's own vocabulary, named here rather than repeated as
/// literals at each comparison site. It is deliberately NOT the Prisma enum:
/// this is an external producer's contract, and coupling it to our persistence
/// model would let a schema change to one silently redefine the other.
export const EVENT_TYPE = {
  DELTA: 'program.capacity.delta',
  SNAPSHOT: 'program.capacity.snapshot',
} as const;

export const DELTA_DIRECTION = { RESERVE: 'RESERVE', RELEASE: 'RELEASE' } as const;
export type DeltaDirection = (typeof DELTA_DIRECTION)[keyof typeof DELTA_DIRECTION];

export const SNAPSHOT_POSITION_STATUS = { RESERVED: 'RESERVED', RELEASED: 'RELEASED' } as const;

/// Only version the consumer accepts; an unknown version fails the union.
export const SUPPORTED_SCHEMA_VERSION = 1;

/// Bounds match the VarChar widths the values land in, so an over-long field is
/// rejected at the edge rather than by Postgres.
const REF_MAX_LENGTH = 64;
const CURRENCY_CODE_LENGTH = 3;

const Decimalish = z.string().regex(/^-?\d{1,20}(\.\d{1,4})?$/);
const Ref = z.string().min(1).max(REF_MAX_LENGTH);
const CurrencyCode = z.string().length(CURRENCY_CODE_LENGTH);

const snapshotPosition = z.object({
  invoice_ref: Ref,
  local_ledger_seq: z.coerce.bigint().nullable(),
  reserved_amount: Decimalish,
  currency: CurrencyCode,
  status: z.enum([SNAPSHOT_POSITION_STATUS.RESERVED, SNAPSHOT_POSITION_STATUS.RELEASED]),
  occurred_at: z.string().datetime({ offset: true }),
});

const deltaEvent = z.object({
  event_id: Ref,
  event_type: z.literal(EVENT_TYPE.DELTA),
  schema_version: z.literal(SUPPORTED_SCHEMA_VERSION),
  produced_at: z.string().datetime({ offset: true }),
  program_ref: Ref,

  /// Treasury's own per-programme sequence number for this event, recorded on
  /// the ledger entry as `treasury_event_seq` and compared against a
  /// snapshot's `included_through_event_seq` during reconciliation.
  ///
  /// Optional on purpose. Making it required would send every delta from a
  /// producer that does not yet emit it straight to the dead-letter table.
  /// A null sequence means "unknown whether treasury has folded this in", and
  /// reconciliation replays such entries rather than dropping them — the same
  /// deliberate over-replay bias described in ADR-06.
  event_seq: z.coerce.bigint().nullable().default(null),

  delta: z.object({
    amount: Decimalish,
    currency: CurrencyCode,
    direction: z.enum([DELTA_DIRECTION.RESERVE, DELTA_DIRECTION.RELEASE]),
  }),
});

const snapshotEvent = z.object({
  event_id: Ref,
  event_type: z.literal(EVENT_TYPE.SNAPSHOT),
  schema_version: z.literal(SUPPORTED_SCHEMA_VERSION),
  produced_at: z.string().datetime({ offset: true }),
  program_ref: Ref,
  program_currency: CurrencyCode,
  snapshot: z.object({
    snapshot_id: Ref,
    snapshot_seq: z.coerce.bigint(),
    as_of: z.string().datetime({ offset: true }),
    total_limit: Decimalish,
    reserved_amount: Decimalish,
    acknowledged_local_seq: z.coerce.bigint().nullable(),
    included_through_event_seq: z.coerce.bigint().nullable(),
    position_count: z.number().int().nonnegative(),
    chunk_index: z.number().int().nonnegative().default(0),
    chunk_count: z.number().int().positive().default(1),
    positions: z.array(snapshotPosition).default([]),
    positions_uri: z.string().url().nullable().default(null),
  }),
});

export const capacityEventEnvelope = z.discriminatedUnion('event_type', [
  deltaEvent,
  snapshotEvent,
]);
export type CapacityEvent = z.infer<typeof capacityEventEnvelope>;
export type SnapshotEvent = z.infer<typeof snapshotEvent>;
