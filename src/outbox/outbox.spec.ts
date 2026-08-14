import { buildReservationEvent } from './outbox';

describe('buildReservationEvent', () => {
  it('carries the ledger seq so treasury can echo it back as an ack watermark', () => {
    const event = buildReservationEvent({
      programRef: 'PRG-1',
      invoiceRef: 'INV-1',
      seq: 42n,
      amount: '10.00',
      currency: 'GBP',
      occurredAt: new Date('2026-01-01T00:00:00Z'),
    });
    expect(event.payload.local_ledger_seq).toBe('42');
    expect(event.payload.event_type).toBe('invoice.reserved');
    expect(event.key).toBe('PRG-1');
  });
});
