import { PostgreSqlContainer, StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { execSync } from 'child_process';
import { PrismaClient } from '@prisma/client';

jest.setTimeout(120_000);

describe('DB-level guarantees (real Postgres)', () => {
  let container: StartedPostgreSqlContainer;
  let prisma: PrismaClient;

  beforeAll(async () => {
    container = await new PostgreSqlContainer('postgres:16-alpine').start();
    const url = container.getConnectionUri();
    execSync('npx prisma migrate deploy', { env: { ...process.env, DATABASE_URL: url } });
    prisma = new PrismaClient({ datasources: { db: { url } } });
    await prisma.currency.createMany({
      data: [
        { code: 'GBP', minorUnits: 2, name: 'Pound Sterling' },
        { code: 'USD', minorUnits: 2, name: 'US Dollar' },
      ],
    });
  });

  afterAll(async () => {
    await prisma.$disconnect();
    await container.stop();
  });

  it('rejects a reservation that exceeds the program limit', async () => {
    const program = await prisma.program.create({
      data: {
        externalRef: 'PRG-1',
        name: 'Test',
        currencyCode: 'GBP',
        totalLimit: '100.00',
        reservedAmount: '0.00',
      },
    });
    await expect(
      prisma.program.update({
        where: { id: program.id },
        data: { reservedAmount: '150.00' },
      }),
    ).rejects.toThrow(/program_no_overcommit/);
  });

  it('rejects direct UPDATE of a ledger entry (append-only trigger)', async () => {
    const program = await prisma.program.create({
      data: { externalRef: 'PRG-2', name: 'Test', currencyCode: 'GBP', totalLimit: '100.00' },
    });
    const entry = await prisma.capacityLedgerEntry.create({
      data: {
        programId: program.id,
        seq: 1,
        entryType: 'RESERVE',
        origin: 'API',
        deltaReserved: '10.00',
        balanceReservedAfter: '10.00',
        balanceLimitAfter: '100.00',
        occurredAt: new Date(),
      },
    });
    await expect(
      prisma.capacityLedgerEntry.update({
        where: { id: entry.id },
        data: { deltaReserved: '999.00' },
      }),
    ).rejects.toThrow(/append-only/);
  });

  it('rejects a face amount with more decimals than the currency allows', async () => {
    const program = await prisma.program.create({
      data: { externalRef: 'PRG-3', name: 'Test', currencyCode: 'GBP', totalLimit: '100.00' },
    });
    await expect(
      prisma.invoice.create({
        data: {
          programId: program.id,
          externalRef: 'INV-1',
          currencyCode: 'GBP',
          faceAmount: '10.999', // 3dp on a 2dp currency
        },
      }),
    ).rejects.toThrow(/invoice_face_quantised/);
  });
});
