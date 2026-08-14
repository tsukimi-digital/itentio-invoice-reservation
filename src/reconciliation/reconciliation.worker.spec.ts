import { computeBaseline, nextCursor } from './reconciliation.worker';
import { Dec } from '../money/decimal';

describe('computeBaseline', () => {
  it('adds the unacknowledged local tail to the snapshot reserved amount', () => {
    const result = computeBaseline({
      treasuryReserved: new Dec('100.00'),
      replayedSum: new Dec('15.00'),
      localReservedBefore: new Dec('90.00'),
    });
    expect(result.newReserved.toString()).toBe('115');
    expect(result.baselineDelta.toString()).toBe('25');
  });

  it('produces a zero delta when nothing needs replaying', () => {
    const result = computeBaseline({
      treasuryReserved: new Dec('100.00'),
      replayedSum: new Dec('0'),
      localReservedBefore: new Dec('100.00'),
    });
    expect(result.baselineDelta.toString()).toBe('0');
  });
});

describe('syncPositions cursor logic', () => {
  it('advances the cursor to the last processed invoiceRef in a page', () => {
    // Pure logic extracted for testability — see nextCursor below.
    const page = [{ invoiceRef: 'INV-001' }, { invoiceRef: 'INV-002' }, { invoiceRef: 'INV-003' }];
    expect(nextCursor(page)).toBe('INV-003');
  });

  it('returns null for an empty page (job complete)', () => {
    expect(nextCursor([])).toBeNull();
  });
});
