// The Prisma CLI stops auto-loading .env once a config file like this one
// exists — `prisma migrate deploy`/`db seed` would otherwise fail to find
// DATABASE_URL. The app itself is unaffected: NestJS's ConfigModule loads
// .env independently at runtime, this only covers bare `prisma` CLI calls.
import 'dotenv/config';
import { defineConfig } from 'prisma/config';

export default defineConfig({
  schema: 'prisma/schema.prisma',
  migrations: {
    seed: 'ts-node prisma/seed.ts',
  },
});
