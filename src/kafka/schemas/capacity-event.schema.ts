import { z } from 'zod';

// Multi-schema topic (deltas + snapshots) via a oneOf-style discriminated
// union, so an unknown event_type fails closed rather than being coerced.
const Decimalish = z.string().regex(/^-?\d{1,20}(\.\d{1,4})?$/);

const snapshotPosition = z.object({
  invoice_ref: z.string().min(1).max(64),
  local_ledger_seq: z.coerce.bigint().nullable(),
  reserved_amount: Decimalish,
  currency: z.string().length(3),
  status: z.enum(['RESERVED', 'RELEASED']),
  occurred_at: z.string().datetime({ offset: true }),
});

const deltaEvent = z.object({
  event_id: z.string().min(1).max(64),
  event_type: z.literal('program.capacity.delta'),
  schema_version: z.literal(1),
  produced_at: z.string().datetime({ offset: true }),
  program_ref: z.string().min(1).max(64),

  /// Treasury's own per-programme sequence number for this event, recorded on
  /// the ledger entry as `treasury_event_seq` and compared against a
  /// snapshot's `included_through_event_seq` during reconciliation.
  ///
  /// Optional on purpose. Making it required would send every delta from a
  /// producer that does not yet emit it straight to the dead-letter table.
  /// A null sequence means "unknown whether treasury has folded this in", and
  /// reconciliation replays such entries rather than dropping them — see
  /// ADR-026 and the deliberate over-replay bias in ADR-004/005.
  event_seq: z.coerce.bigint().nullable().default(null),

  delta: z.object({
    amount: Decimalish,
    currency: z.string().length(3),
    direction: z.enum(['RESERVE', 'RELEASE']),
  }),
});

const snapshotEvent = z.object({
  event_id: z.string().min(1).max(64),
  event_type: z.literal('program.capacity.snapshot'),
  schema_version: z.literal(1),
  produced_at: z.string().datetime({ offset: true }),
  program_ref: z.string().min(1).max(64),
  program_currency: z.string().length(3),
  snapshot: z.object({
    snapshot_id: z.string().min(1).max(64),
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
