import { capacityEventEnvelope } from './capacity-event.schema';

describe('capacityEventEnvelope', () => {
  it('accepts a well-formed incremental delta event', () => {
    const result = capacityEventEnvelope.safeParse({
      event_id: 'evt-1',
      event_type: 'program.capacity.delta',
      schema_version: 1,
      produced_at: '2026-01-01T00:00:00Z',
      program_ref: 'PRG-1',
      delta: { amount: '10.00', currency: 'GBP', direction: 'RESERVE' },
    });
    expect(result.success).toBe(true);
  });

  it('rejects a payload missing event_id', () => {
    const result = capacityEventEnvelope.safeParse({ event_type: 'program.capacity.delta' });
    expect(result.success).toBe(false);
  });

  it('accepts a snapshot event with fencing fields', () => {
    const result = capacityEventEnvelope.safeParse({
      event_id: 'evt-2',
      event_type: 'program.capacity.snapshot',
      schema_version: 1,
      produced_at: '2026-01-01T00:00:00Z',
      program_ref: 'PRG-1',
      program_currency: 'GBP',
      snapshot: {
        snapshot_id: 'snap-1',
        snapshot_seq: '1',
        as_of: '2026-01-01T00:00:00Z',
        total_limit: '1000.00',
        reserved_amount: '100.00',
        acknowledged_local_seq: '5',
        included_through_event_seq: null,
        position_count: 0,
        chunk_index: 0,
        chunk_count: 1,
        positions: [],
        positions_uri: null,
      },
    });
    expect(result.success).toBe(true);
  });
});
