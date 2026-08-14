import { PrismaClient } from '@prisma/client';
import { scryptSync, randomBytes } from 'node:crypto';

const prisma = new PrismaClient();

function hashPassword(plain: string): string {
  const salt = randomBytes(16);
  const hash = scryptSync(plain, salt, 64);
  return `scrypt$16384$8$1$${salt.toString('base64')}$${hash.toString('base64')}`;
}

async function main() {
  await prisma.currency.createMany({
    data: [
      { code: 'GBP', minorUnits: 2, name: 'Pound Sterling' },
      { code: 'USD', minorUnits: 2, name: 'US Dollar' },
      { code: 'EUR', minorUnits: 2, name: 'Euro' },
      { code: 'JPY', minorUnits: 0, name: 'Japanese Yen' },
    ],
    skipDuplicates: true,
  });

  const now = new Date();
  await prisma.fxRate.createMany({
    data: [
      { baseCurrencyCode: 'EUR', quoteCurrencyCode: 'GBP', rate: '0.855000000000', source: 'SEED', asOf: now },
      { baseCurrencyCode: 'USD', quoteCurrencyCode: 'GBP', rate: '0.790000000000', source: 'SEED', asOf: now },
      { baseCurrencyCode: 'JPY', quoteCurrencyCode: 'GBP', rate: '0.005200000000', source: 'SEED', asOf: now },
    ],
    skipDuplicates: true,
  });

  await prisma.user.upsert({
    where: { email: 'admin@itentio.dev' },
    update: {},
    create: {
      email: 'admin@itentio.dev',
      passwordHash: hashPassword('dev-only-password-change-me'),
      displayName: 'Dev Admin',
      role: 'ADMIN',
    },
  });
}

main().finally(() => prisma.$disconnect());
