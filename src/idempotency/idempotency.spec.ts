import { claimIdempotencyKey } from './idempotency';
import type { Tx } from './idempotency';

function makeTx(
  overrides: Partial<{
    $executeRaw: jest.Mock;
    idempotencyKey: { findUniqueOrThrow: jest.Mock };
  }> = {},
) {
  return {
    $executeRaw: jest.fn().mockResolvedValue(1),
    idempotencyKey: { findUniqueOrThrow: jest.fn() },
    ...overrides,
  } as unknown as Tx;
}

describe('claimIdempotencyKey', () => {
  it('claims ownership when the insert succeeds', async () => {
    const tx = makeTx();
    const result = await claimIdempotencyKey(tx, {
      rowId: 'id1',
      clientId: 'c1',
      key: 'k1',
      method: 'POST',
      path: '/x',
      requestHash: 'h1',
    });
    expect(result.kind).toBe('owned');
  });

  it('replays a completed response for the same hash', async () => {
    const tx = makeTx({
      $executeRaw: jest.fn().mockResolvedValue(0),
      idempotencyKey: {
        findUniqueOrThrow: jest.fn().mockResolvedValue({
          requestHash: 'h1',
          status: 'COMPLETED',
          responseBody: { ok: true },
        }),
      },
    });
    const result = await claimIdempotencyKey(tx, {
      rowId: 'id1',
      clientId: 'c1',
      key: 'k1',
      method: 'POST',
      path: '/x',
      requestHash: 'h1',
    });
    expect(result).toEqual({ kind: 'replay', storedResponse: { ok: true } });
  });

  it('flags a conflict when the same key carries a different request hash', async () => {
    const tx = makeTx({
      $executeRaw: jest.fn().mockResolvedValue(0),
      idempotencyKey: {
        findUniqueOrThrow: jest
          .fn()
          .mockResolvedValue({ requestHash: 'different', status: 'IN_FLIGHT', responseBody: null }),
      },
    });
    const result = await claimIdempotencyKey(tx, {
      rowId: 'id1',
      clientId: 'c1',
      key: 'k1',
      method: 'POST',
      path: '/x',
      requestHash: 'h1',
    });
    expect(result.kind).toBe('conflict');
  });
});
