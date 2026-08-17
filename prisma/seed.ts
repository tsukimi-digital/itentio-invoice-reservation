import { PrismaClient } from '@prisma/client';
import { hashPassword } from '../src/auth/password';

const prisma = new PrismaClient();

const DEV_ADMIN_EMAIL = 'admin@itentio.dev';
const DEV_ADMIN_PASSWORD = 'dev-only-password-change-me';

/// The admin fixture is a development convenience, and `prisma db seed` is a
/// command someone can run anywhere — including against production, where it
/// would create an ADMIN account whose password is printed in the README.
/// Outside development the password must be supplied explicitly, or no user
/// is created at all.
async function seedAdminUser(): Promise<void> {
  const isProduction = process.env.NODE_ENV === 'production';
  const suppliedPassword = process.env.SEED_ADMIN_PASSWORD;

  if (isProduction && !suppliedPassword) {
    console.warn(
      'NODE_ENV=production and SEED_ADMIN_PASSWORD is unset — skipping admin user. ' +
        'Set SEED_ADMIN_PASSWORD to seed one deliberately.',
    );
    return;
  }

  const email = process.env.SEED_ADMIN_EMAIL ?? DEV_ADMIN_EMAIL;
  const password = suppliedPassword ?? DEV_ADMIN_PASSWORD;

  await prisma.user.upsert({
    where: { email },
    update: {},
    create: {
      email,
      passwordHash: hashPassword(password),
      displayName: 'Dev Admin',
      role: 'ADMIN',
    },
  });
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

  // Two programmes, so a reviewer can exercise the API — including the
  // cross-currency path the brief calls out — straight after seeding, without
  // hand-creating a row first. `PRG-1` carries the limit used as the example in
  // the assignment; `PRG-2` is denominated in a zero-decimal currency, so a GBP
  // invoice against it converts and rounds to whole yen.
  await prisma.program.createMany({
    data: [
      {
        externalRef: 'PRG-1',
        name: 'Demo Supply Chain Programme',
        currencyCode: 'GBP',
        totalLimit: '10000000.0000',
      },
      {
        externalRef: 'PRG-2',
        name: 'Demo JPY Programme',
        currencyCode: 'JPY',
        totalLimit: '1500000000.0000',
      },
    ],
    skipDuplicates: true,
  });

  await seedAdminUser();
}

// A failed seed must exit non-zero: `main().finally(...)` alone swallows the
// rejection and still exits 0, so a broken seed looks like a successful one —
// including in CI, which runs `prisma db seed` before the test suite.
main()
  .catch((error: unknown) => {
    console.error('Seed failed:', error);
    process.exitCode = 1;
  })
  .finally(() => prisma.$disconnect());
