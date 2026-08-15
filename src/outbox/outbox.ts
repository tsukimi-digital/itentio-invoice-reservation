import { randomUUID } from 'node:crypto';

export const RESERVATION_EVENTS_TOPIC = 'capacity.reservation-events';

export interface OutboxEvent {
  topic: string;
  key: string;
  payload: Record<string, unknown>;
}

interface LedgerEventInput {
  programRef: string;
  invoiceRef: string;
  seq: bigint;
  amount: string;
  currency: string;
  occurredAt: Date;
}

/// Every outbound event carries the local ledger `seq` it corresponds to.
/// Treasury echoes the highest one it has folded into its reported
/// `reserved_amount` as `acknowledged_local_seq`, and reconciliation replays
/// only entries above that watermark (ADR-004/005).
///
/// That contract is only sound if EVERY local `seq` is published. Releases
/// used to allocate a ledger `seq` and publish nothing, so treasury could
/// acknowledge seq 3 while never having seen the release at seq 2 —
/// reconciliation then skipped the release as "already included" and restored
/// capacity that had in fact been given back. See docs/DECISIONS.md ADR-022.
function buildLedgerEvent(eventType: string, input: LedgerEventInput): OutboxEvent {
  return {
    topic: RESERVATION_EVENTS_TOPIC,
    // Partition key is the programme, so a programme's events keep their
    // relative order on the wire.
    key: input.programRef,
    payload: {
      event_id: randomUUID(),
      event_type: eventType,
      schema_version: 1,
      produced_at: new Date().toISOString(),
      program_ref: input.programRef,
      invoice_ref: input.invoiceRef,
      local_ledger_seq: input.seq.toString(),
      amount: input.amount,
      currency: input.currency,
      occurred_at: input.occurredAt.toISOString(),
    },
  };
}

export function buildReservationEvent(input: LedgerEventInput): OutboxEvent {
  return buildLedgerEvent('invoice.reserved', input);
}

/// `amount` is the programme-currency amount being returned, expressed
/// positive; `event_type` carries the direction.
export function buildReleaseEvent(input: LedgerEventInput): OutboxEvent {
  return buildLedgerEvent('invoice.released', input);
}
