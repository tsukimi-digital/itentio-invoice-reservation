import { PostgreSqlContainer, StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { execSync } from 'child_process';
import { PrismaClient } from '@prisma/client';
import { CapacityService } from '../src/capacity/capacity.service';
import { FxService } from '../src/fx/fx.service';
import type { PrismaService } from '../src/prisma/prisma.service';
import type { FxRateProvider } from '../src/fx/fx-rate.provider';
import { randomUUID } from 'node:crypto';

jest.setTimeout(120_000);

describe('Concurrent reservations never overcommit (real Postgres)', () => {
  let container: StartedPostgreSqlContainer;
  let prisma: PrismaClient;
  let service: CapacityService;

  beforeAll(async () => {
    container = await new PostgreSqlContainer('postgres:16-alpine').start();
    const url = container.getConnectionUri();
    execSync('npx prisma migrate deploy', { env: { ...process.env, DATABASE_URL: url } });
    prisma = new PrismaClient({ datasources: { db: { url } } });

    // Same-currency reservations never call the FX provider — see FxService's
    // identity short-circuit — so a provider that always throws is safe here.
    const unusedProvider: FxRateProvider = {
      resolve: () => Promise.reject(new Error('unused in this test')),
    };
    service = new CapacityService(
      prisma as unknown as PrismaService,
      new FxService(unusedProvider),
    );

    await prisma.currency.create({ data: { code: 'GBP', minorUnits: 2, name: 'GBP' } });
  });

  afterAll(async () => {
    await prisma.$disconnect();
    await container.stop();
  });

  it('rejects excess reservations under 50-way concurrency on a 100.00 limit', async () => {
    const program = await prisma.program.create({
      data: {
        externalRef: 'PRG-CONC',
        name: 'Concurrency',
        currencyCode: 'GBP',
        totalLimit: '100.00',
      },
    });

    // 50 concurrent attempts to reserve 5.00 each = 250.00 total demand
    // against a 100.00 limit. Exactly 20 must succeed, 30 must fail.
    const attempts = Array.from({ length: 50 }, (_, i) =>
      service
        .reserve(
          {
            programRef: program.externalRef,
            invoiceRef: `INV-${i}`,
            amount: '5.00',
            currency: 'GBP',
            requestedAt: new Date(),
          },
          {
            rowId: randomUUID(),
            clientId: 'c1',
            key: randomUUID(),
            method: 'POST',
            path: '/x',
            requestHash: 'h',
          },
        )
        .then(() => 'ok' as const)
        .catch(() => 'rejected' as const),
    );

    const results = await Promise.all(attempts);
    const succeeded = results.filter((r) => r === 'ok').length;
    expect(succeeded).toBe(20);

    const final = await prisma.program.findUniqueOrThrow({ where: { id: program.id } });
    expect(final.reservedAmount.toFixed(2)).toBe('100.00'); // never exceeds the limit

    const ledgerSum = await prisma.capacityLedgerEntry.aggregate({
      where: { programId: program.id },
      _sum: { deltaReserved: true },
    });
    expect(ledgerSum._sum.deltaReserved?.toFixed(2)).toBe('100.00'); // ledger and balance agree
  });
});
