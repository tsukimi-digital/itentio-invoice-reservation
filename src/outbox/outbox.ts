import { randomUUID } from 'node:crypto';

export interface OutboxEvent {
  topic: string;
  key: string;
  payload: Record<string, unknown>;
}

export function buildReservationEvent(input: {
  programRef: string;
  invoiceRef: string;
  seq: bigint;
  amount: string;
  currency: string;
  occurredAt: Date;
}): OutboxEvent {
  return {
    topic: 'capacity.reservation-events',
    key: input.programRef,
    payload: {
      event_id: randomUUID(),
      event_type: 'invoice.reserved',
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
