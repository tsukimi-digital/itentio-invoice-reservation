import { Test } from '@nestjs/testing';
import { CapacityService } from './capacity.service';
import { PrismaService } from '../prisma/prisma.service';
import { FxService } from '../fx/fx.service';
import { InsufficientCapacityException } from './exceptions';
import { Dec } from '../money/decimal';

describe('CapacityService.reserve', () => {
  it('throws InsufficientCapacityException when the guarded UPDATE returns zero rows', async () => {
    const queryRaw = jest
      .fn()
      .mockResolvedValueOnce([
        {
          id: 'p1',
          currency_code: 'GBP',
          total_limit: new Dec('100'),
          reserved_amount: new Dec('95'),
          next_ledger_seq: 5n,
          status: 'ACTIVE',
        },
      ]) // program row lock
      .mockResolvedValueOnce([]); // guarded UPDATE: 0 rows => insufficient
    const tx = {
      $executeRawUnsafe: jest.fn(),
      $queryRaw: queryRaw,
      $executeRaw: jest.fn().mockResolvedValue(1),
      idempotencyKey: { findUniqueOrThrow: jest.fn() },
      invoice: {
        findUnique: jest.fn().mockResolvedValue(null),
        create: jest.fn().mockResolvedValue({ id: 'inv1' }),
      },
    };
    const transaction = jest.fn((fn: (tx: unknown) => unknown) => fn(tx));
    const prisma = {
      $transaction: transaction,
      program: {
        findUniqueOrThrow: jest
          .fn()
          .mockResolvedValue({ id: 'p1', currencyCode: 'GBP', currency: { minorUnits: 2 } }),
      },
      currency: { findUniqueOrThrow: jest.fn().mockResolvedValue({ code: 'GBP', minorUnits: 2 }) },
    };
    const fx = {
      convert: jest.fn().mockResolvedValue({
        amount: {
          toDecimal: () => new Dec('10'),
          currency: 'GBP',
          toJSON: () => ({ amount: '10.00', currency: 'GBP' }),
        },
        audit: {
          rateId: null,
          effectiveRate: new Dec('1'),
          source: 'IDENTITY',
          asOf: new Date(),
          inverted: false,
          pivot: null,
        },
      }),
    };

    const module = await Test.createTestingModule({
      providers: [
        CapacityService,
        { provide: PrismaService, useValue: prisma },
        { provide: FxService, useValue: fx },
      ],
    }).compile();
    const service = module.get(CapacityService);

    await expect(
      service.reserve(
        {
          programRef: 'PRG-1',
          invoiceRef: 'INV-1',
          amount: '10.00',
          currency: 'GBP',
          requestedAt: new Date(),
        },
        { rowId: 'ik1', clientId: 'c1', key: 'k1', method: 'POST', path: '/x', requestHash: 'h1' },
      ),
    ).rejects.toThrow(InsufficientCapacityException);
  });
});
