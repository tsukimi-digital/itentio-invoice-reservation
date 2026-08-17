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
      currency: { findUnique: jest.fn().mockResolvedValue({ code: 'GBP', minorUnits: 2 }) },
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

  // Quantising the face amount with the PROGRAMME currency's minor units
  // would silently round a GBP invoice against a JPY programme (minorUnits 0)
  // from 1234.56 to 1235 before conversion, over-reserving capacity and
  // persisting a false face amount.
  describe('cross-currency quantisation', () => {
    function buildHarness(invoiceMinorUnits: number) {
      const invoiceCreate = jest.fn().mockResolvedValue({ id: 'inv1' });
      const tx = {
        $executeRawUnsafe: jest.fn(),
        $executeRaw: jest.fn().mockResolvedValue(1),
        $queryRaw: jest
          .fn()
          .mockResolvedValueOnce([
            {
              id: 'p1',
              currency_code: 'JPY',
              total_limit: new Dec('10000000'),
              reserved_amount: new Dec('0'),
              next_ledger_seq: 1n,
              status: 'ACTIVE',
            },
          ])
          .mockResolvedValueOnce([
            {
              reserved_amount: new Dec('237415'),
              total_limit: new Dec('10000000'),
              assigned_seq: 1n,
            },
          ]),
        idempotencyKey: { findUniqueOrThrow: jest.fn(), update: jest.fn().mockResolvedValue({}) },
        invoice: {
          findUnique: jest.fn().mockResolvedValue(null),
          create: invoiceCreate,
          update: jest.fn().mockResolvedValue({
            id: 'inv1',
            externalRef: 'INV-1',
            status: 'RESERVED',
            reservedProgramAmount: new Dec('237415'),
          }),
        },
        capacityLedgerEntry: { create: jest.fn().mockResolvedValue({}) },
        outboxMessage: { create: jest.fn().mockResolvedValue({}) },
      };
      const prisma = {
        $transaction: jest.fn((fn: (t: unknown) => unknown) => fn(tx)),
        program: {
          findUniqueOrThrow: jest.fn().mockResolvedValue({
            id: 'p1',
            currencyCode: 'JPY',
            currency: { minorUnits: 0 },
          }),
        },
        currency: {
          findUnique: jest.fn().mockResolvedValue({ code: 'GBP', minorUnits: invoiceMinorUnits }),
        },
      };
      const fx = {
        convert: jest.fn().mockResolvedValue({
          amount: {
            toDecimal: () => new Dec('237415'),
            toString: () => '237415',
            currency: 'JPY',
            toJSON: () => ({ amount: '237415', currency: 'JPY' }),
          },
          audit: {
            rateId: 'r1',
            effectiveRate: new Dec('0.0052'),
            source: 'SEED',
            asOf: new Date(),
            inverted: true,
            pivot: null,
          },
        }),
      };
      return { tx, prisma, fx, invoiceCreate };
    }

    async function serviceFor(harness: ReturnType<typeof buildHarness>) {
      const module = await Test.createTestingModule({
        providers: [
          CapacityService,
          { provide: PrismaService, useValue: harness.prisma },
          { provide: FxService, useValue: harness.fx },
        ],
      }).compile();
      return module.get(CapacityService);
    }

    const command = {
      programRef: 'PRG-JPY',
      invoiceRef: 'INV-1',
      amount: '1234.56',
      currency: 'GBP',
      requestedAt: new Date(),
    };
    const idem = {
      rowId: 'ik1',
      clientId: 'c1',
      key: 'k1',
      method: 'POST',
      path: '/x',
      requestHash: 'h1',
    };

    it('quantises the face amount with the invoice currency scale, not the programme scale', async () => {
      const harness = buildHarness(2);
      const service = await serviceFor(harness);

      await service.reserve(command, idem);

      const invoiceCalls = harness.invoiceCreate.mock.calls as [
        { data: { faceAmount: Dec; currencyCode: string } },
      ][];
      const created = invoiceCalls[0][0];
      expect(created.data.faceAmount.toString()).toBe('1234.56');
      expect(created.data.currencyCode).toBe('GBP');

      // The Money handed to the FX layer must carry the full invoice amount.
      const convertCalls = harness.fx.convert.mock.calls as [{ toString(): string }][];
      expect(convertCalls[0][0].toString()).toBe('1234.56');
    });

    it('rejects an amount with more precision than the invoice currency allows', async () => {
      // Invoice currency with 0 minor units — "1234.56" is not roundable, it
      // is malformed, and must not be silently booked as a different number.
      const harness = buildHarness(0);
      const service = await serviceFor(harness);

      await expect(service.reserve(command, idem)).rejects.toThrow(/more precision/);
      expect(harness.invoiceCreate).not.toHaveBeenCalled();
    });
  });
});

describe('CapacityService.release', () => {
  it('is idempotent — releasing an already-released invoice is a no-op', async () => {
    const queryRaw = jest
      .fn()
      .mockResolvedValueOnce([{ id: 'p1', status: 'ACTIVE' }])
      .mockResolvedValueOnce([
        { id: 'inv1', status: 'RELEASED', reserved_program_amount: new Dec('10') },
      ]);
    const tx = {
      $executeRawUnsafe: jest.fn(),
      $executeRaw: jest.fn().mockResolvedValue(1),
      $queryRaw: queryRaw,
      idempotencyKey: { findUniqueOrThrow: jest.fn(), update: jest.fn().mockResolvedValue({}) },
    };
    const transaction = jest.fn((fn: (tx: unknown) => unknown) => fn(tx));
    const prisma = {
      $transaction: transaction,
      program: { findUniqueOrThrow: jest.fn().mockResolvedValue({ id: 'p1' }) },
    };

    const module = await Test.createTestingModule({
      providers: [
        CapacityService,
        { provide: PrismaService, useValue: prisma },
        { provide: FxService, useValue: {} },
      ],
    }).compile();
    const service = module.get(CapacityService);

    const result = (await service.release(
      { programRef: 'PRG-1', invoiceRef: 'INV-1' },
      { rowId: 'ik2', clientId: 'c1', key: 'k2', method: 'POST', path: '/x', requestHash: 'h2' },
    )) as { status: string };
    expect(result.status).toBe('RELEASED');
  });
});
