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
/// only entries above that watermark.
///
/// That contract is only sound if EVERY local `seq` is published, releases
/// included. A ledger `seq` allocated but never published lets treasury
/// acknowledge seq 3 without having seen the release at seq 2 — reconciliation
/// then skips that release as "already included" and restores capacity that
/// was in fact given back.
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
